package com.psddev.dari.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.TypeReference;

class BootstrapImportTask extends Task {

    public static final String EXECUTOR_PREFIX = "Bootstrap Import";

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapImportTask.class);

    private final Database database;
    private final String filename;
    private final InputStream fileInputStream;
    private final boolean deleteFirst;
    private final int queueSize;
    private final int numWriters;
    private final int commitSize;
    private final AsyncQueue<Record> saveQueue;
    private final List<AsyncDatabaseWriter<Record>> savers = new ArrayList<AsyncDatabaseWriter<Record>>();
    private final List<AsyncDatabaseWriter<Record>> deleters = new ArrayList<AsyncDatabaseWriter<Record>>();
    private AsyncQueue<Record> deleteQueue;
    private final Map<UUID, ObjectType> unknownTypes = new HashMap<UUID, ObjectType>();
    private boolean needsTranslation;
    private final Pattern uuidPattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
    private final Map<UUID, UUID> remoteToLocalIdMap = new HashMap<UUID, UUID>();
    private final Map<String, String> remoteToLocalIdStringMap = new HashMap<String, String>();
    private static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_TYPE = new TypeReference<Map<String, Object>>() { };

    public BootstrapImportTask(Database database, String filename, InputStream fileInputStream, boolean deleteFirst, int numWriters, int commitSize) {
        super(EXECUTOR_PREFIX, EXECUTOR_PREFIX + " " + filename);
        this.database = database;
        this.filename = filename;
        this.fileInputStream = fileInputStream;
        this.deleteFirst = deleteFirst;
        this.numWriters = numWriters;
        this.commitSize = commitSize;
        this.queueSize = numWriters * commitSize;
        this.saveQueue = new AsyncQueue<Record>(new ArrayBlockingQueue<Record>(queueSize));
        if (deleteFirst) {
            this.deleteQueue = new AsyncQueue<Record>(new ArrayBlockingQueue<Record>(queueSize));
        }
    }

    public void doTask() throws IOException {
        List<Task> tasks = new ArrayList<Task>();
        try {
            for (int i = 0; i < numWriters; i++) {
                AsyncDatabaseWriter<Record> saver = new AsyncDatabaseWriter<Record>(EXECUTOR_PREFIX, saveQueue, database, WriteOperation.SAVE_UNSAFELY, commitSize, true);
                savers.add(saver);
                saver.submit();
                if (deleteFirst) {
                    AsyncDatabaseWriter<Record> deleter = new AsyncDatabaseWriter<Record>("Bootstrap Delete " + filename, deleteQueue, database, WriteOperation.DELETE, commitSize, true);
                    deleters.add(deleter);
                    deleter.submit();
                }
            }
            tasks.addAll(deleters);
            tasks.addAll(savers);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
            String line;
            // get headers
            // read leading blank lines
            do {
                line = reader.readLine();
                if (line != null) {
                    line = line.trim();
                }
            } while ("".equals(line));

            Map<String, String> headers = new HashMap<String, String>();
            do {
                if (line != null) {
                    line = line.trim();
                }
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    headers.put(parts[0].trim(), parts[1].trim());
                }
            } while (!"".equals((line = reader.readLine())));

            if (deleteFirst) {
                if (!headers.containsKey(BootstrapPackage.Static.TYPES_HEADER) || headers.get(BootstrapPackage.Static.TYPES_HEADER) == null || "".equals(headers.get(BootstrapPackage.Static.TYPES_HEADER).trim())) {
                    throw new RuntimeException("Missing " + BootstrapPackage.Static.TYPES_HEADER + " header");
                }
            }
            if (!(!headers.containsKey(BootstrapPackage.Static.ROW_COUNT_HEADER)
                    || headers.get(BootstrapPackage.Static.ROW_COUNT_HEADER) == null
                    || "".equals(headers.get(BootstrapPackage.Static.ROW_COUNT_HEADER).trim()))) {
                setProgressTotal(ObjectUtils.to(Long.class, headers.get(BootstrapPackage.Static.ROW_COUNT_HEADER)));
            }
            UUID localObjTypeId = database.getEnvironment().getTypeByClass(ObjectType.class).getId();
            UUID globalsId = new UUID(-1L, -1L);
            Set<String> typeNames = new HashSet<String>();
            boolean isAllTypes = false;
            Map<String, String> typeMapTypeFields = new HashMap<String, String>();
            if (headers.get(BootstrapPackage.Static.TYPES_HEADER).trim().equals(BootstrapPackage.Static.ALL_TYPES_HEADER_VALUE)) {
                isAllTypes = true;
                if (deleteFirst) {
                    LOGGER.info("Deleting all records in database to load " + filename);
                    for (Object obj : Query.fromAll().where("_type != ?", localObjTypeId).and("_id != ?", globalsId).noCache().using(database).resolveToReferenceOnly().iterable(100)) {
                        if (!shouldContinue()) {
                            break;
                        }
                        if (obj instanceof Record) {
                            deleteQueue.add((Record) obj);
                        }
                    }
                }
            } else {
                List<UUID> typeIds = new ArrayList<UUID>();
                for (String remoteTypeName : headers.get(BootstrapPackage.Static.TYPES_HEADER).split(",")) {
                    remoteTypeName = remoteTypeName.trim();
                    ObjectType localType = database.getEnvironment().getTypeByName(remoteTypeName);
                    typeNames.add(remoteTypeName);
                    if (localType == null) {
                        localType = new ObjectType();
                        localType.setInternalName(remoteTypeName);
                        localType.setDisplayName("Unknown Type: " + remoteTypeName);
                        unknownTypes.put(localType.getId(), localType);
                    } else {
                        if (localType.getGroups().contains(Singleton.class.getName())) {
                            typeMapTypeFields.put(remoteTypeName, "dari.singleton.key");
                        } else {
                            typeIds.add(localType.getId());
                        }
                    }
                }
                if (deleteFirst) {
                    LOGGER.info("Deleting all records of types specified in " + filename);
                    for (Object obj : Query.fromAll().where("_type = ?", typeIds).and("_type != ?", localObjTypeId).and("_id != ?", globalsId).noCache().using(database).resolveToReferenceOnly().iterable(100)) {
                        if (!shouldContinue()) {
                            break;
                        }
                        if (obj instanceof Record) {
                            deleteQueue.add((Record) obj);
                        }
                    }
                }

                if (headers.containsKey(BootstrapPackage.Static.TYPE_MAP_HEADER) && headers.get(BootstrapPackage.Static.TYPE_MAP_HEADER) != null && !"".equals(headers.get(BootstrapPackage.Static.TYPE_MAP_HEADER).trim())) {
                    for (String typeMap : headers.get(BootstrapPackage.Static.TYPE_MAP_HEADER).split(",")) {
                        String[] parts = typeMap.split("/");
                        if (parts.length == 2) {
                            typeMapTypeFields.put(parts[0], parts[1]);
                        }
                    }
                }

            }

            // block until deleters are done
            if (deleteQueue != null) {
                deleteQueue.closeAutomatically();
                boolean done = false;
                while (!done) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    if (!shouldContinue()) {
                        for (Task deleter : deleters) {
                            deleter.stop();
                        }
                        break;
                    }
                    done = true;
                    for (Task deleter : deleters) {
                        if (deleter.isRunning()) {
                            done = false;
                            break;
                        }
                    }
                }
            }
            LOGGER.info("Importing data from " + filename + " . . . ");
            int numRows = 0;
            boolean afterObjectTypes = false;
            ObjectType objType = database.getEnvironment().getTypeByClass(ObjectType.class);
            while (null != (line = reader.readLine())) {
                if (!shouldContinue()) {
                    break;
                }
                line = line.trim();
                if ("".equals(line)) {
                    continue;
                }
                if (line.startsWith("#")) {
                    continue;
                }
                if (!line.startsWith("{") || !line.endsWith("}")) {
                    throw new RuntimeException("Invalid line in input file: " + line);
                }
                line = translateIds(line);
                Map<String, Object> stateMap = ObjectUtils.to(MAP_STRING_OBJECT_TYPE, ObjectUtils.fromJson(line));
                UUID globalId = new UUID(-1L, -1L);
                UUID zeroTypeId = new UUID(0L, 0L);
                try {
                    UUID id = ObjectUtils.to(UUID.class, stateMap.get("_id"));
                    if (id == null) {
                        LOGGER.error("Invalid line in input file: " + line);
                        continue;
                    }
                    if (id.equals(globalId)) {
                        LOGGER.debug("Not importing " + globalId);
                        continue;
                    }
                    ObjectType type = database.getEnvironment().getTypeByName(ObjectUtils.to(String.class, stateMap.get("_type")));
                    if (type == null) {
                        if (zeroTypeId.equals(ObjectUtils.to(UUID.class, stateMap.get("_type")))) {
                            LOGGER.debug("Not importing type " + zeroTypeId);
                        } else {
                            LOGGER.error("Unknown type in line: " + line);
                        }
                        continue;
                    }
                    Object obj = type.createObject(id);
                    Record record;
                    if (obj instanceof Record) {
                        record = (Record) obj;
                    } else {
                        LOGGER.error("Unknown type in line: " + line);
                        continue;
                    }

                    if (!afterObjectTypes && !objType.equals(type)) {
                        afterObjectTypes = true;
                        // this is the first line after all of the objectTypes, potentially need to re-parse it.
                        prepareIdTranslation();
                        line = translateIds(line);
                        stateMap = ObjectUtils.to(MAP_STRING_OBJECT_TYPE, ObjectUtils.fromJson(line));
                    }

                    if ((typeMapTypeFields.containsKey(ObjectUtils.to(String.class, stateMap.get("_type"))) || objType.equals(type))) {
                        String typeMapField = typeMapTypeFields.get(ObjectUtils.to(String.class, stateMap.get("_type")));
                        Object localObj;
                        if (objType.equals(type)) {
                            localObj = database.getEnvironment().getTypeByName(ObjectUtils.to(String.class, stateMap.get("internalName")));
                        } else {
                            localObj = Query.fromType(type).where(typeMapField + " = ?", ObjectUtils.to(String.class, stateMap.get(typeMapField))).first();
                        }
                        if (localObj instanceof Recordable) {
                            UUID localId = ((Recordable) localObj).getState().getId();
                            remoteToLocalIdMap.put(ObjectUtils.to(UUID.class, stateMap.get("_id")), localId);
                            stateMap.put("_id", localId);
                        }
                        if (localObj == null || isAllTypes || typeNames.contains(type.getInternalName())) {
                            record.getState().setResolveToReferenceOnly(true);
                            record.getState().setValues(stateMap);
                            saveQueue.add(record);
                        }
                    } else {
                        record.getState().setResolveToReferenceOnly(true);
                        record.getState().setValues(stateMap);
                        saveQueue.add(record);
                    }

                    setProgressIndex(++numRows);
                } catch (RuntimeException t) {
                    LOGGER.error("Error when saving state at " + stateMap.get("_id") + ": ", t);
                }
            }
        } catch (RuntimeException e) {
            for (Task task : tasks) {
                task.stop();
            }
            throw e;
        } finally {
            // block until all tasks are done
            saveQueue.closeAutomatically();
            fileInputStream.close();
            if (deleteQueue != null) {
                deleteQueue.closeAutomatically();
            }
            boolean done = false;
            while (!done) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    continue;
                }
                if (!shouldContinue()) {
                    for (Task task : tasks) {
                        task.stop();
                    }
                    break;
                }
                done = true;
                for (Task saver : savers) {
                    if (saver.isRunning()) {
                        done = false;
                        break;
                    }
                }
            }
            LOGGER.info("Done with import of " + filename + ".");
        }
    }

    private void prepareIdTranslation() {
        needsTranslation = !remoteToLocalIdMap.isEmpty();
        for (Map.Entry<UUID, UUID> entry : remoteToLocalIdMap.entrySet()) {
            remoteToLocalIdStringMap.put(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    private String translateIds(String line) {
        if (!needsTranslation) {
            return line;
        }
        StringBuilder newLine = new StringBuilder();
        Matcher idMatcher = uuidPattern.matcher(line);
        int cursor = 0;
        boolean found = false;
        while (idMatcher.find()) {
            int start = idMatcher.start();
            int end = idMatcher.end();
            newLine.append(line.substring(cursor, start));
            String remoteId = line.substring(start, end);
            String localId;
            if ((localId = remoteToLocalIdStringMap.get(remoteId)) != null) {
                found = true;
                newLine.append(localId);
            } else {
                newLine.append(remoteId);
            }
            cursor = end;
            ObjectType unknownType;
            if ((unknownType = unknownTypes.remove(localId)) != null) {
                saveQueue.add(unknownType);
            }
        }
        if (found) {
            newLine.append(line.substring(cursor));
            return newLine.toString();
        } else {
            return line;
        }
    }

}
