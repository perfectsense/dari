package com.psddev.dari.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.joda.time.DateTime;

import com.psddev.dari.util.ObjectUtils;

public class BootstrapPackage extends Record {

    @Indexed(unique = true)
    private String name;

    private Set<ObjectType> types;

    private boolean isInit;

    private Map<ObjectType, Set<BootstrapPackage>> typesInOtherPackages;

    private Map<ObjectType, Set<ObjectField>> missingTypes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<ObjectType> getTypes() {
        if (types == null) {
            types = new HashSet<ObjectType>();
        }
        return types;
    }

    public void setTypes(Set<ObjectType> types) {
        this.types = types;
    }

    public boolean isInit() {
        return isInit;
    }

    public void setIsInit(Boolean isInit) {
        this.isInit = isInit;
    }

    public Map<ObjectType, Set<BootstrapPackage>> getTypesInOtherPackages() {
        if (typesInOtherPackages == null) {
            typesInOtherPackages = new TreeMap<ObjectType, Set<BootstrapPackage>>();
        }
        return typesInOtherPackages;
    }

    public void setTypesInOtherPackages(Map<ObjectType, Set<BootstrapPackage>> typesInOtherPackages) {
        this.typesInOtherPackages = typesInOtherPackages;
    }

    public Map<ObjectType, Set<ObjectField>> getMissingTypes() {
        if (missingTypes == null) {
            missingTypes = new TreeMap<ObjectType, Set<ObjectField>>();
        }
        return missingTypes;
    }

    public void setMissingTypes(Map<ObjectType, Set<ObjectField>> missingTypes) {
        this.missingTypes = missingTypes;
    }

    @TypeData.FieldInternalNamePrefix("bootstrap.")
    public static class TypeData extends Modification<ObjectType> {

        private Set<String> packages;

        private Set<String> typeMappableGroups;

        private String typeMappableUniqueKey;

        private Set<String> followReferencesFields;

        public Set<String> getPackageNames() {
            if (packages == null) {
                packages = new HashSet<String>();
            }
            return packages;
        }

        public void setPackageNames(Set<String> packages) {
            this.packages = packages;
        }

        public Set<String> getTypeMappableGroups() {
            if (typeMappableGroups == null) {
                typeMappableGroups = new HashSet<String>();
            }
            return typeMappableGroups;
        }

        public void setTypeMappableGroups(Set<String> typeMappableGroups) {
            this.typeMappableGroups = typeMappableGroups;
        }

        public String getTypeMappableUniqueKey() {
            return typeMappableUniqueKey;
        }

        public void setTypeMappableUniqueKey(String typeMappableUniqueKey) {
            this.typeMappableUniqueKey = typeMappableUniqueKey;
        }

        public Set<String> getFollowReferencesFields() {
            if (followReferencesFields == null) {
                followReferencesFields = new HashSet<String>();
            }
            return followReferencesFields;
        }

        public void setFollowReferencesFields(Set<String> followReferencesFields) {
            this.followReferencesFields = followReferencesFields;
        }

    }

    public static final class Static {
        public static final String INIT_NAME = " _init";

        public static final String PACKAGE_NAME_HEADER = "Bootstrap Package Name";
        public static final String PROJECT_HEADER = "Project";
        public static final String DATE_HEADER = "Date";
        public static final String TYPES_HEADER = "Types";
        public static final String TYPE_MAP_HEADER = "Mapping Types";
        public static final String ROW_COUNT_HEADER = "Row Count";
        public static final String ALL_TYPES_HEADER_VALUE = "ALL";

        private static final int MAX_SEEN_REFERENCE_IDS_SIZE = 100000;

        public static BootstrapPackage getPackage(Database database, String name) {
            return getPackagesMap(database).get(name);
        }

        public static List<BootstrapPackage> getPackages(Database database) {
            return Collections.unmodifiableList(new ArrayList<BootstrapPackage>(getPackagesMap(database).values()));
        }

        private static Map<String, BootstrapPackage> getPackagesMap(Database database) {
            Map<String, BootstrapPackage> packagesByName = new TreeMap<String, BootstrapPackage>();
            // First create the _init package for the whole database
            BootstrapPackage initPkg = new BootstrapPackage();
            initPkg.setName(INIT_NAME);
            initPkg.setIsInit(true);
            packagesByName.put(INIT_NAME, initPkg);

            for (ObjectType type : database.getEnvironment().getTypes()) {
                TypeData bmod = type.as(TypeData.class);
                for (String name : bmod.getPackageNames()) {
                    BootstrapPackage pkg = packagesByName.get(name);
                    if (pkg == null) {
                        pkg = new BootstrapPackage();
                        pkg.setName(name);
                        packagesByName.put(name, pkg);
                    }
                    pkg.getTypes().add(type);
                }
            }
            for (BootstrapPackage pkg : Query.from(BootstrapPackage.class).using(database).selectAll()) {
                packagesByName.put(pkg.getName(), pkg);
            }
            for (Map.Entry<String, BootstrapPackage> entry : packagesByName.entrySet()) {
                checkConsistency(database, entry.getValue(), new HashSet<BootstrapPackage>(packagesByName.values()), null);
            }
            return packagesByName;
        }

        public static void checkConsistency(Database database, BootstrapPackage pkg, Set<BootstrapPackage> allPackages, Set<ObjectType> additionalTypes) {
            Set<ObjectType> allMyTypes = getAllTypes(database, pkg);
            Set<ObjectType> allTypes = new HashSet<ObjectType>();
            for (BootstrapPackage otherPkg : allPackages) {
                allTypes.addAll(getAllTypes(database, otherPkg));
            }
            Map<ObjectField, ObjectType> checkFields = new HashMap<ObjectField, ObjectType>();
            for (ObjectType type : allMyTypes) {
                for (ObjectField field : type.getFields()) {
                    for (ObjectType t : field.getTypes()) {
                        if (!field.isEmbedded() && !t.isEmbedded()) {
                            checkFields.put(field, t);
                        }
                    }
                }
            }

            if (additionalTypes != null) {
                for (ObjectType type : additionalTypes) {
                    for (ObjectField field : type.getFields()) {
                        for (ObjectType t : field.getTypes()) {
                            if (!field.isEmbedded() && !t.isEmbedded()) {
                                checkFields.put(field, t);
                            }
                        }
                    }
                }
            }

            for (ObjectField field : database.getEnvironment().getFields()) {
                for (ObjectType t : field.getTypes()) {
                    if (!field.isEmbedded() && !t.isEmbedded()) {
                        checkFields.put(field, t);
                    }
                }
            }

            for (Map.Entry<ObjectField, ObjectType> entry : checkFields.entrySet()) {
                ObjectField field = entry.getKey();
                ObjectType t = entry.getValue();
                if (field.getParentType() != null
                        && field.getParentType().as(TypeData.class).getFollowReferencesFields().contains(field.getInternalName())) {
                    continue;
                }
                if (!allMyTypes.contains(t)) {
                    if (allTypes.contains(t)) {
                        if (!pkg.getTypesInOtherPackages().containsKey(t)) {
                            pkg.getTypesInOtherPackages().put(t, new HashSet<BootstrapPackage>());
                        }
                        for (BootstrapPackage otherPkg : allPackages) {
                            if (getAllTypes(database, otherPkg).contains(t)) {
                                pkg.getTypesInOtherPackages().get(t).add(otherPkg);
                            }
                        }
                    }
                    if (t.as(TypeData.class).getTypeMappableGroups().isEmpty() || t.as(TypeData.class).getTypeMappableUniqueKey() == null) {
                        if (!pkg.getMissingTypes().containsKey(t)) {
                            pkg.getMissingTypes().put(t, new HashSet<ObjectField>());
                        }
                        pkg.getMissingTypes().get(t).add(field);
                    }
                }
            }
        }

        public static Set<ObjectType> getAllTypes(Database database, BootstrapPackage pkg) {
            return getAllTypes(database, pkg.getTypes());
        }

        public static Set<ObjectType> getAllTypes(Database database, Set<ObjectType> types) {
            Set<ObjectType> allTypes = new HashSet<ObjectType>();
            for (ObjectType type : types) {
                allTypes.addAll(database.getEnvironment().getTypesByGroup(type.getInternalName()));
            }
            return allTypes;
        }

        public static void writeContents(Database database, BootstrapPackage pkg, Set<ObjectType> additionalTypes, Writer writer, String projectName) throws IOException {
            boolean first = true;
            boolean needsObjectTypeMap = false;
            Set<ObjectType> typeMaps = new HashSet<ObjectType>();
            ObjectType objType = database.getEnvironment().getTypeByClass(ObjectType.class);
            Set<ObjectType> exportTypes = new HashSet<ObjectType>();
            Query<?> query = Query.fromAll().using(database);
            Set<ObjectType> allTypeMappableTypes = new HashSet<ObjectType>();
            Set<UUID> concreteTypeIds = new HashSet<UUID>();

            for (ObjectType type : database.getEnvironment().getTypes()) {
                if (!type.as(TypeData.class).getTypeMappableGroups().isEmpty()
                        && type.as(TypeData.class).getTypeMappableUniqueKey() != null) {
                    allTypeMappableTypes.add(type);
                }
            }

            // Package:
            writer.write(PACKAGE_NAME_HEADER + ": ");
            writer.write(pkg.getName());
            writer.write('\n');
            // Project:
            writer.write(PROJECT_HEADER + ": ");
            writer.write(projectName);
            writer.write('\n');
            // Date:
            writer.write(DATE_HEADER + ": ");
            writer.write(new DateTime().toString("yyyy-MM-dd HH:mm:ss z"));
            writer.write('\n');

            // Types:
            writer.write(TYPES_HEADER + ": ");
            if (pkg.isInit()) {
                writer.write(ALL_TYPES_HEADER_VALUE);
                writer.write('\n');
            } else {
                exportTypes.addAll(getAllTypes(database, pkg));
                exportTypes.addAll(getAllTypes(database, additionalTypes));
                for (ObjectType typeMappableType : allTypeMappableTypes) {
                    GETTYPEMAPPABLETYPE: for (ObjectType type : exportTypes) {
                        for (String group : typeMappableType.as(TypeData.class).getTypeMappableGroups()) {
                            if (type.getGroups().contains(group)) {
                                typeMaps.add(typeMappableType);
                                break GETTYPEMAPPABLETYPE;
                            }
                        }
                    }
                }
                GETOBJECTTYPE: for (ObjectType type : exportTypes) {
                    for (ObjectField field : type.getFields()) {
                        if (field.getTypes().contains(objType)) {
                            needsObjectTypeMap = true;
                            break GETOBJECTTYPE;
                        }
                    }
                }

                for (ObjectType exportType : exportTypes) {
                    String clsName = exportType.getObjectClassName();
                    if (!ObjectUtils.isBlank(clsName)) {
                        for (ObjectType concreteType : database.getEnvironment().getTypesByGroup(clsName)) {
                            if (!concreteType.isAbstract()) {
                                concreteTypeIds.add(concreteType.getId());
                            }
                        }
                    }
                }

                if (exportTypes.contains(objType)) {
                    if (!first) {
                        writer.write(',');
                    } else {
                        first = false;
                    }
                    writer.write(objType.getInternalName());
                }
                for (ObjectType type : exportTypes) {
                    if (type.equals(objType)) {
                        continue;
                    }
                    if (!first) {
                        writer.write(',');
                    } else {
                        first = false;
                    }
                    writer.write(type.getInternalName());
                }
                writer.write('\n');

            }

            // Determine if there are any fields that need references followed
            Map<UUID, Map<String, ObjectType>> followReferences = new HashMap<UUID, Map<String, ObjectType>>();
            if (!pkg.isInit()) {
                for (ObjectType type : exportTypes) {
                    for (String fieldName : type.as(TypeData.class).getFollowReferencesFields()) {
                        ObjectField field = type.getField(fieldName);
                        if (field != null) {
                            for (ObjectType fieldType : field.getTypes()) {
                                if (!exportTypes.contains(fieldType)) {
                                    if (!followReferences.containsKey(type.getId())) {
                                        followReferences.put(type.getId(), new HashMap<String, ObjectType>());
                                    }
                                    followReferences.get(type.getId()).put(fieldName, fieldType);
                                }
                            }
                        }
                    }
                }
            }

            if (!typeMaps.isEmpty()) {
                for (ObjectType typeMapType : typeMaps) {
                    String clsName = typeMapType.getObjectClassName();
                    if (!ObjectUtils.isBlank(clsName)) {
                        for (ObjectType type : database.getEnvironment().getTypesByGroup(clsName)) {
                            concreteTypeIds.remove(type.getId());
                        }
                    }
                }
            }

            // Mapping Types:
            if (pkg.isInit() || needsObjectTypeMap || !typeMaps.isEmpty()) {
                first = true;
                writer.write(TYPE_MAP_HEADER + ": ");
                if (pkg.isInit() || needsObjectTypeMap) {
                    writer.write(objType.getInternalName());
                    writer.write("/internalName");
                    first = false;
                }
                for (ObjectType typeMapType : typeMaps) {
                    if (!first) {
                        writer.write(',');
                    } else {
                        first = false;
                    }
                    writer.write(typeMapType.getInternalName());
                    writer.write('/');
                    writer.write(typeMapType.as(TypeData.class).getTypeMappableUniqueKey());
                }
                writer.write('\n');
            }

            // Row Count:
            Long count = 0L;
            try {
                if (concreteTypeIds.isEmpty()) {
                    count = Query.fromAll().using(database).noCache().count();
                } else {
                    for (UUID concreteTypeId : concreteTypeIds) {
                        long concreteCount = Query.fromAll().using(database).noCache().where("_type = ?", concreteTypeId).count();
                        count = count + concreteCount;
                    }
                    if (needsObjectTypeMap) {
                        long objectCount = Query.fromAll().using(database).noCache().where("_type = ?", objType).count();
                        count = count + objectCount;
                    }
                    if (!typeMaps.isEmpty()) {
                        for (ObjectType typeMapType : typeMaps) {
                            long typeMapCount = Query.fromAll().using(database).noCache().where("_type = ?", typeMapType).count();
                            count = count + typeMapCount;
                        }
                    }
                }
                writer.write(ROW_COUNT_HEADER + ": ");
                writer.write(ObjectUtils.to(String.class, count));
                writer.write('\n');
            } catch (RuntimeException e) {
                // Count query timed out. Just don't write the row count header.
                count = null;
            }

            // blank line between headers and data
            writer.write('\n');
            writer.flush();

            // ObjectType records first
            if (exportTypes.isEmpty() || exportTypes.contains(objType) || needsObjectTypeMap) {
                for (Object r : Query.fromType(objType).using(database).noCache().resolveToReferenceOnly().iterable(100)) {
                    writer.write(ObjectUtils.toJson(((Recordable) r).getState().getSimpleValues(true)));
                    writer.write('\n');
                }
            }

            // Then other mapping types
            for (ObjectType typeMapType : typeMaps) {
                for (Object r : Query.fromType(typeMapType).using(database).noCache().resolveToReferenceOnly().iterable(100)) {
                    writer.write(ObjectUtils.toJson(((Recordable) r).getState().getSimpleValues(true)));
                    writer.write('\n');
                }
            }

            // Then everything else
            if (pkg.isInit()) {
                concreteTypeIds.clear(); // should already be empty
                concreteTypeIds.add(null);
            }
            UUID lastTypeId = null;
            Set<UUID> seenIds = new HashSet<UUID>();
            query.getOptions().put(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);
            for (UUID typeId : concreteTypeIds) {
                Query<?> concreteQuery = query.clone();
                if (typeId != null) {
                    concreteQuery.where("_type = ?", typeId);
                }
                for (Object o : concreteQuery.noCache().resolveToReferenceOnly().iterable(100)) {
                    if (o instanceof Recordable) {
                        Recordable r = (Recordable) o;
                        writer.write(ObjectUtils.toJson(r.getState().getSimpleValues(true)));
                        writer.write('\n');
                        if (!pkg.isInit()) {
                            if (lastTypeId == null || !lastTypeId.equals(r.getState().getTypeId())) {
                                seenIds.clear();
                            } else if (seenIds.size() > MAX_SEEN_REFERENCE_IDS_SIZE) {
                                seenIds.clear();
                            }
                            lastTypeId = r.getState().getTypeId();
                            Map<String, ObjectType> followReferencesFieldMap;
                            if ((followReferencesFieldMap = followReferences.get(r.getState().getTypeId())) != null) {
                                for (Map.Entry<String, ObjectType> entry : followReferencesFieldMap.entrySet()) {
                                    Object reference = r.getState().getRawValue(entry.getKey());
                                    Set<UUID> referenceIds = new HashSet<UUID>();
                                    if (reference instanceof Collection) {
                                        for (Object referenceObj : ((Collection<?>) reference)) {
                                            if (referenceObj instanceof Recordable) {
                                                UUID referenceUUID = ObjectUtils.to(UUID.class, ((Recordable) referenceObj).getState().getId());
                                                if (referenceUUID != null) {
                                                    if (!seenIds.contains(referenceUUID)) {
                                                        referenceIds.add(referenceUUID);
                                                    }
                                                }
                                            }
                                        }
                                    } else if (reference instanceof Recordable) {
                                        UUID referenceUUID = ObjectUtils.to(UUID.class, ((Recordable) reference).getState().getId());
                                        if (referenceUUID != null) {
                                            if (!seenIds.contains(referenceUUID)) {
                                                referenceIds.add(referenceUUID);
                                            }
                                        }
                                    }
                                    if (!referenceIds.isEmpty()) {
                                        for (Object ref : Query.fromType(entry.getValue()).noCache().using(database).where("_id = ?", referenceIds).selectAll()) {
                                            if (ref instanceof Recordable) {
                                                Recordable refr = (Recordable) ref;
                                                seenIds.add(refr.getState().getId());
                                                writer.write(ObjectUtils.toJson(refr.getState().getSimpleValues(true)));
                                                writer.write('\n');
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            writer.flush();
        }

        public static void importContents(Database database, String filename, InputStream fileInputStream, boolean deleteFirst, int numWriters, int commitSize) throws IOException {
            BootstrapImportTask importer = new BootstrapImportTask(database, filename, fileInputStream, deleteFirst, numWriters, commitSize);
            importer.submit();
        }
    }
}
