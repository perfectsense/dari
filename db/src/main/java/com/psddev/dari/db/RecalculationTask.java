package com.psddev.dari.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.RepeatingTask;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.Stats;
import com.psddev.dari.util.StringUtils;

/**
 * Periodically updates indexes annotated with {@code \@Recalculate}.
 *
 * Optionally specify a task host with the setting "dari/recalculationTaskHost".
 */
public class RecalculationTask extends RepeatingTask {

    private static final int UPDATE_LATEST_EVERY_SECONDS = 60;
    private static final int QUERY_ITERABLE_SIZE = 200;
    private static final int COMMIT_SIZE = 200;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecalculationTask.class);
    private static final Stats STATS = new Stats("Recalculation Task");
    private static final String TASK_HOST_SETTING = "dari/recalculationTaskHost";

    private String processingKey;

    @Override
    protected DateTime calculateRunTime(DateTime currentTime) {
        return everyMinute(currentTime);
    }

    @Override
    protected void doRepeatingTask(DateTime runTime) throws Exception {
        processingKey = null;

        String hostname = Settings.get(String.class, TASK_HOST_SETTING);
        if (!isTaskHost(hostname)) {
            return;
        }

        for (RecalculationContext context : getIndexableMethods()) {
            Stats.Timer timer = STATS.startTimer();
            long recalculated = recalculateIfNecessary(context);
            if (recalculated > 0L) {
                timer.stop("Recalculate " + context.getKey(), recalculated);
                // only process one method per task execution so progressIndex / progressTotal makes sense
                break;
            }
        }
    }

    /**
     * Checks the LastRecalculation and DistributedLock and executes recalculate if it should.
     */
    private long recalculateIfNecessary(RecalculationContext context) {
        String updateKey = context.getKey();
        long recalculated = 0;
        boolean shouldExecute = false;
        boolean canExecute = false;
        LastRecalculation last = Query.from(LastRecalculation.class).master().noCache().where("key = ?", updateKey).first();

        if (last == null) {
            last = new LastRecalculation();
            last.setKey(updateKey);
            shouldExecute = true;

        } else {
            if (last.getLastExecutedDate() == null && last.getCurrentRunningDate() == null) {
                // this has never been executed, and it's not currently executing.
                shouldExecute = true;

            } else if (last.getLastExecutedDate() != null && context.delay.isUpdateDue(new DateTime(), last.getLastExecutedDate())) {
                // this has been executed before and an update is due.
                shouldExecute = true;
            }

            if (last.getCurrentRunningDate() != null) {
                // this is currently executing
                shouldExecute = false;
                if (context.delay.isUpdateDue(new DateTime(), last.getCurrentRunningDate())) {
                    // the task is running, but another update is already due.
                    // It probably died. Clear it out and pick it up next
                    // time if it hasn't updated, but don't run this time.
                    last.setCurrentRunningDate(null);
                    last.saveImmediately();
                }
            }
        }

        if (shouldExecute) {
            // Check to see if any other processes are currently running on other hosts.
            if (Query.from(LastRecalculation.class).where("currentRunningDate > ?", new DateTime().minusSeconds(UPDATE_LATEST_EVERY_SECONDS * 5)).hasMoreThan(0)) {
                shouldExecute = false;
            }
        }

        if (shouldExecute) {
            boolean locked = false;
            DistributedLock lock = new DistributedLock(Database.Static.getDefault(), updateKey);
            try {
                if (lock.tryLock()) {
                    locked = true;
                    last.setCurrentRunningDate(new DateTime());
                    last.saveImmediately();
                    canExecute = true;
                }

            } finally {
                if (locked) {
                    lock.unlock();
                }
            }

            Long startTime = System.nanoTime();
            if (canExecute) {
                try {
                    processingKey = context.getKey();
                    LOGGER.info("Recalculating " + processingKey);
                    recalculated = recalculate(context, last);

                } finally {
                    LOGGER.info("Recalculated: " + getProgress());
                    last.setLastExecutedDate(new DateTime());
                    last.setCurrentRunningDate(null);
                    last.setRecalculatedCount(getProgressIndex());
                    last.setExecutionTimeSeconds((System.nanoTime() - startTime) / 1_000_000_000L);
                    last.saveImmediately();
                }
            }
        }
        return recalculated;
    }

    /**
     * Actually does the work of iterating through the records and updating indexes.
     */
    private long recalculate(final RecalculationContext context, LastRecalculation last) {

        LoadingCache<ObjectType, ObjectIndex[]> typeMethodIndexes = CacheBuilder.newBuilder().build(
                new CacheLoader<ObjectType, ObjectIndex[]>() {
                    @Override
                    public ObjectIndex[] load(ObjectType type) throws Exception {
                        Set<ObjectIndex> result = new HashSet<ObjectIndex>();
                        for (ObjectMethod method : context.methods) {
                            result.addAll(method.findIndexes(type));
                        }
                        return result.toArray(new ObjectIndex[result.size()]);
                    }
                }
        );

        long recalculated = 0L;
        int transactionCounter = 0;
        setProgressIndex(0L);
        Database db = Database.Static.getDefault();
        db.beginWrites();
        try {
            ObjectField metricField = context.getMetric();
            DateTime processedLastRunDate = last.getLastExecutedDate();
            boolean isGlobal = context.groups.contains(Object.class.getName());
            Iterator<?> iterator;

            if (metricField != null) {
                if (last.getLastExecutedDate() != null) {
                    MetricInterval interval = metricField.as(MetricAccess.FieldData.class).getEventDateProcessor();
                    if (interval != null) {
                        processedLastRunDate = new DateTime(interval.process(processedLastRunDate));
                    }
                    if (context.delay != null) {
                        processedLastRunDate = context.delay.metricAfterDate(processedLastRunDate);
                    }

                } else if (context.delay != null) {
                    processedLastRunDate = context.delay.metricAfterDate(new DateTime());
                }
                LOGGER.info("Recalculating " + context.getKey() + " after " + processedLastRunDate);
                iterator = Metric.Static.getDistinctIdsBetween(Database.Static.getDefault(), null, metricField, processedLastRunDate, null);

            } else {
                Query<?> query = Query.fromAll().using(db).noCache().resolveToReferenceOnly().option(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);
                if (!isGlobal) {
                    Set<ObjectType> concreteTypes = new HashSet<>();
                    for (String group : context.groups) {
                        concreteTypes.addAll(db.getEnvironment().getTypesByGroup(group).stream().filter(ObjectType::isConcrete).collect(Collectors.toSet()));
                    }
                    if (!concreteTypes.isEmpty()) {
                        query.where("_type = ?", concreteTypes);
                    }
                }
                iterator = query.iterable(QUERY_ITERABLE_SIZE).iterator();
            }

            UUID lastId = null;
            while (iterator.hasNext()) {
                try {
                    Object obj = iterator.next();
                    if (!shouldContinue()) {
                        break;
                    }
                    if (obj instanceof Metric.DistinctIds) {
                        Metric.DistinctIds distinctIds = (Metric.DistinctIds) obj;
                        if (distinctIds.id.equals(lastId)) {
                            continue;
                        }
                        lastId = distinctIds.id;
                        obj = Query.fromAll().noCache().resolveToReferenceOnly().where("_id = ?", distinctIds.id).first();
                    }
                    setProgressIndex(getProgressIndex() + 1);
                    State objState = State.getInstance(obj);
                    if (objState == null || objState.getType() == null) {
                        continue;
                    }
                    if (!isGlobal) {
                        Set<String> objGroups = new HashSet<String>(objState.getType().getGroups());
                        objGroups.retainAll(context.groups);
                        if (objGroups.isEmpty()) {
                            continue;
                        }
                    }
                    objState.setResolveToReferenceOnly(false);
                    ObjectIndex[] indexes = typeMethodIndexes.getUnchecked(objState.getType());
                    db.recalculate(objState, indexes);
                    recalculated += indexes.length;
                    transactionCounter += indexes.length;
                    if (transactionCounter >= COMMIT_SIZE) {
                        transactionCounter = 0;
                        db.commitWritesEventually();
                    }

                } finally {
                    if (last.getCurrentRunningDate().plusSeconds(UPDATE_LATEST_EVERY_SECONDS).isBeforeNow()) {
                        last.setCurrentRunningDate(new DateTime());
                        last.saveImmediately();
                    }
                }
            }

            if (transactionCounter > 0) {
                db.commitWritesEventually();
            }
            last.setCurrentRunningDate(new DateTime());
            last.saveImmediately();

        } finally {
            db.endWrites();
        }
        return recalculated;
    }

    @Override
    public String getProgress() {
        StringBuilder progress = new StringBuilder();
        String key = processingKey;

        if (key != null) {
            progress.append(key).append(' ');
        }

        String superProgress = super.getProgress();

        if (superProgress != null) {
            progress.append(superProgress);
        }

        return progress.toString();
    }

    private static boolean isTaskHost(String hostname) {
        if (hostname == null || "localhost".equals(hostname)) {
            return true;
        }
        try {
            InetAddress allowed = InetAddress.getByName(hostname);
            InetAddress local = InetAddress.getLocalHost();
            return local.getHostAddress().equals(allowed.getHostAddress());
        } catch (UnknownHostException e) {
            LOGGER.error("Unknown host exception during Recalculation", e);
            return false;
        }
    }

    /**
     * Saves the last time the index update was executed for each context.
     */
    public static class LastRecalculation extends Record {

        // null if it's not running
        @Indexed
        private Long currentRunningDate;

        private Long lastExecutedDate;

        @Indexed(unique = true)
        private String key;

        private Long recalculatedCount;

        private Long executionTimeSeconds;

        public DateTime getCurrentRunningDate() {
            return (currentRunningDate == null ? null : new DateTime(currentRunningDate));
        }

        public void setCurrentRunningDate(DateTime currentRunningDate) {
            this.currentRunningDate = (currentRunningDate == null ? null : currentRunningDate.getMillis());
        }

        public DateTime getLastExecutedDate() {
            return (lastExecutedDate == null ? null : new DateTime(lastExecutedDate));
        }

        public void setLastExecutedDate(DateTime lastExecutedDate) {
            this.lastExecutedDate = (lastExecutedDate == null ? null : lastExecutedDate.getMillis());
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Long getRecalculatedCount() {
            return recalculatedCount;
        }

        public void setRecalculatedCount(Long recalculatedCount) {
            this.recalculatedCount = recalculatedCount;
        }

        public Long getExecutionTimeSeconds() {
            return executionTimeSeconds;
        }

        public void setExecutionTimeSeconds(Long executionTimeSeconds) {
            this.executionTimeSeconds = executionTimeSeconds;
        }
    }

    private static Collection<RecalculationContext> getIndexableMethods() {
        Map<String, RecalculationContext> contextsByGroupsAndDelayAndMetric = new HashMap<String, RecalculationContext>();

        for (ObjectType type : Database.Static.getDefault().getEnvironment().getTypes()) {
            for (ObjectMethod method : type.getMethods()) {
                if (method.getJavaDeclaringClassName().equals(type.getObjectClassName())
                        && !method.as(RecalculationFieldData.class).isImmediate()
                        && method.as(RecalculationFieldData.class).getRecalculationDelay() != null) {

                    TreeSet<String> groups = new TreeSet<String>();
                    Set<Class<?>> objectClasses = new HashSet<Class<?>>();
                    for (String group : method.as(RecalculationFieldData.class).getGroups()) {
                        objectClasses.add(ObjectUtils.getClassByName(group));
                    }
                    if (objectClasses.isEmpty()) {
                        objectClasses.add(type.getObjectClass());
                    }
                    for (Class<?> objectClass : objectClasses) {
                        if (Modification.class.isAssignableFrom(objectClass)) {
                            @SuppressWarnings("unchecked")
                            Class<? extends Modification<?>> modClass = ((Class<? extends Modification<?>>) objectClass);
                            for (Class<?> modifiedClass : Modification.Static.getModifiedClasses(modClass)) {
                                ObjectType modifiedType = ObjectType.getInstance(modifiedClass);
                                if (modifiedType != null) {
                                    groups.add(modifiedType.getInternalName());

                                } else {
                                    groups.add(modifiedClass.getName());
                                }
                            }

                        } else {
                            groups.add(objectClass.getName());
                        }
                    }

                    RecalculationContext context = new RecalculationContext(type, groups, method.as(RecalculationFieldData.class).getRecalculationDelay());
                    context.methods.add(method);
                    String key = context.getKey();
                    if (!contextsByGroupsAndDelayAndMetric.containsKey(key)) {
                        contextsByGroupsAndDelayAndMetric.put(key, context);
                    }
                    contextsByGroupsAndDelayAndMetric.get(key).methods.add(method);
                }
            }
        }

        return contextsByGroupsAndDelayAndMetric.values();
    }

    private static final class RecalculationContext {
        public final ObjectType type;
        public final RecalculationDelay delay;
        public final TreeSet<String> groups;
        public final Set<ObjectMethod> methods = new HashSet<ObjectMethod>();

        public RecalculationContext(ObjectType type, TreeSet<String> groups, RecalculationDelay delay) {
            this.type = type;
            this.groups = groups;
            this.delay = delay;
        }

        public ObjectField getMetric() {
            boolean first = true;
            ObjectField metricField = null;
            ObjectField useMetricField = null;
            for (ObjectMethod method : methods) {
                String methodMetricFieldName = method.as(MetricAccess.FieldData.class).getRecalculableFieldName();
                ObjectField methodMetricField = null;
                if (methodMetricFieldName != null) {
                    methodMetricField = type.getState().getDatabase().getEnvironment().getField(methodMetricFieldName);
                    if (methodMetricField == null) {
                        methodMetricField = type.getField(methodMetricFieldName);
                    }
                    if (methodMetricField == null) {
                        LOGGER.warn("Invalid metric field: " + methodMetricFieldName);
                    }
                }
                if (first) {
                    metricField = methodMetricField;
                    useMetricField = metricField;
                } else {
                    if (!ObjectUtils.equals(metricField, methodMetricField)) {
                        useMetricField = null;
                    }
                }
                first = false;
            }
            return useMetricField;
        }

        public String getKey() {
            if (methods.isEmpty()) {
                throw new IllegalStateException("Add a method before you get the key!");
            }

            ObjectField metricField = getMetric();
            return type.getInternalName() + " "
                    + StringUtils.join(groups.toArray(new String[groups.size()]), ",") + " "
                    + delay.getClass().getName()
                    + (metricField != null ? " " + metricField.getUniqueName() : "");
        }
    }
}
