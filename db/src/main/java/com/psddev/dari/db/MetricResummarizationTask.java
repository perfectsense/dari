package com.psddev.dari.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.RepeatingTask;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.Task;
import com.psddev.dari.util.TypeReference;

/** Automatic resummarization of Metric values, driven by the {@code
 * "dari/metricResummarize"} Settings map. To use, add lines like these to your
 * context.xml:
 *
 * <pre>
 * {@code
 * <!-- To match all of the analytics fields -->
 * <Environment name="dari/metricResummarize/analyticsDaily/fields" type="java.lang.String" value="analytics.*" />
 * <!-- Resummarize all data older than 180 days-->
 * <Environment name="dari/metricResummarize/analyticsDaily/beforeDays" type="java.lang.Integer" value="180" />
 * <!-- Collapse it from Hourly (the default) to Daily -->
 * <Environment name="dari/metricResummarize/analyticsDaily/intervalClass" type="java.lang.String" value="com.psddev.dari.db.MetricInterval$Daily" />
 * <!-- Only run on a single host (optional) -->
 * <Environment name="dari/metricResummarize/analyticsDaily/hostname" type="java.lang.String" value="task.host.com" />
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * <!-- To match one or more specific fields: -->
 * <Environment name="dari/metricResummarize/example/fields" type="java.lang.String" value="com.example.ExampleClass/exampleMetricField com.example.ExampleClass2/myOtherMetricField com.example.ExampleClass3/count*" />
 * <!-- Resummarize all data older than 7 days-->
 * <Environment name="dari/metricResummarize/example/beforeDays" type="java.lang.Integer" value="7" />
 * <!-- Collapse it from Hourly (the default) to Daily -->
 * <Environment name="dari/metricResummarize/example/intervalClass" type="java.lang.String" value="com.psddev.dari.db.MetricInterval$Hourly" />
 * <!-- Specify a database (references the dari/database Settings map) -->
 * <Environment name="dari/metricResummarize/example/database" type="java.lang.String" value="mydb" />
 * }
 * </pre>
 *
 * Alternatively, to execute directly:
 *
 * <pre>
 * {@code
 * Database database = Database.Static.getDefault();
 * String key = "analytics";
 * String[] fieldSpecs = {"analytics.*"};
 * int beforeDays = 1;
 * String intervalClassName = "com.psddev.dari.db.MetricInterval$Daily";
 * MetricResummarizationTask.Static.submitResummarizationTask(database, key, fieldSpecs, beforeDays, intervalClassName);
 * }
 * </pre>
 *
 */

public class MetricResummarizationTask extends RepeatingTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricResummarizationTask.class);
    private static final String CONFIG_PREFIX = "dari/metricResummarize";
    private static final String CONFIG_FIELDS = "fields";
    private static final String CONFIG_BEFORE_DAYS = "beforeDays";
    private static final String CONFIG_INTERVAL_CLASS = "intervalClass";
    private static final String CONFIG_DATABASE = "database";
    private static final String CONFIG_HOSTNAME = "hostname";
    private static final Map<String, Map<String, Object>> CONFIG = Settings.get(new TypeReference<Map<String, Map<String, Object>>>() { }, CONFIG_PREFIX);

    @Override
    protected DateTime calculateRunTime(DateTime currentTime) {
        return everyDay(currentTime);
    }

    @Override
    protected void doRepeatingTask(DateTime runTime) throws Exception {
        if (CONFIG == null || CONFIG.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Map<String, Object>> entry : CONFIG.entrySet()) {

            String key = entry.getKey();
            Map<String, Object> settings = entry.getValue();
            String fieldsStr = ObjectUtils.to(String.class, settings.get(CONFIG_FIELDS));

            if (ObjectUtils.isBlank(fieldsStr)) {
                LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_FIELDS + " is required; aborting.");
                continue;
            }
            String[] fieldSpecs = fieldsStr.split("\\s+");

            Integer beforeDays = ObjectUtils.to(Integer.class, settings.get(CONFIG_BEFORE_DAYS));
            if (beforeDays == null) {
                LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_BEFORE_DAYS + " is required; aborting.");
                continue;
            }

            String intervalClassName = ObjectUtils.to(String.class, settings.get(CONFIG_INTERVAL_CLASS));
            if (intervalClassName == null) {
                LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_INTERVAL_CLASS + " is required; aborting.");
                continue;
            }

            Database database = Database.Static.getDefault();
            String databaseName = ObjectUtils.to(String.class, settings.get(CONFIG_DATABASE));
            if (databaseName != null) {
                database = Database.Static.getInstance(databaseName);
                if (database == null) {
                    LOGGER.warn("Metric Resummarization: " + CONFIG_PREFIX + "/" + key + "/" + CONFIG_DATABASE + " is an invalid database; aborting.");
                    continue;
                }
            }
            String hostname = ObjectUtils.to(String.class, settings.get(CONFIG_HOSTNAME));
            if (!isTaskHost(hostname)) {
                continue;
            }

            try {
                Static.submitResummarizationTask(this, database, key, fieldSpecs, beforeDays, intervalClassName);
            } catch (ResummarizationSettingsException e) {
                LOGGER.warn(e.getMessage());
            }
        }
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
            LOGGER.error("Unknown host exception during Metric Resummarization", e);
            return false;
        }
    }

    public static class ResummarizationSettingsException extends Exception {

        private static final long serialVersionUID = 1L;

        private final String key;
        private final String subkey;
        private final String note;

        public ResummarizationSettingsException(String key, String subkey, String note) {
            this.key = key;
            this.subkey = subkey;
            this.note = note;
        }

        public String getMessage() {
            return "Metric Resummarization: " + CONFIG_PREFIX + "/" + key + (subkey != null ? ("/" + subkey) : "") + " " + note;
        }
    }

    public static class LastResummarization extends Record {

        private Long startDate;

        private Long endDate;

        private Long runDate;

        @Indexed(unique = true)
        private String key;

        public DateTime getStartDate() {
            return (startDate == null ? null : new DateTime(startDate));
        }

        public void setStartDate(DateTime startDate) {
            this.startDate = (startDate == null ? null : startDate.getMillis());
        }

        public DateTime getEndDate() {
            return (endDate == null ? null : new DateTime(endDate));
        }

        public void setEndDate(DateTime endDate) {
            this.endDate = (endDate == null ? null : endDate.getMillis());
        }

        public DateTime getRunDate() {
            return (runDate == null ? null : new DateTime(runDate));
        }

        public void setRunDate(DateTime runDate) {
            this.runDate = (runDate == null ? null : runDate.getMillis());
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }

    public static final class Static {

        private Static() { }

        public static void submitResummarizationTask(Database database, String key, String[] fieldSpecs, int beforeDays, String intervalClassName) {
            try {
                submitResummarizationTask(null, database, key, fieldSpecs, beforeDays, intervalClassName);
            } catch (ResummarizationSettingsException e) {
                throw new RuntimeException(e);
            }
        }

        private static void submitResummarizationTask(MetricResummarizationTask parentTask, Database database, String key, String[] fieldSpecs, int beforeDays, String intervalClassName) throws ResummarizationSettingsException {

            Set<ObjectField> fields = resolveFieldSpecs(database, key, fieldSpecs);

            if (fields.isEmpty()) {
                throw new ResummarizationSettingsException(key, null, " specifies no valid fields to resummarize; aborting.");
            }

            MetricInterval interval;
            try {
                @SuppressWarnings("unchecked")
                Class<MetricInterval> intervalClass = (Class<MetricInterval>) Class.forName(intervalClassName);
                interval = intervalClass.newInstance();
            } catch (Exception e) {
                throw new ResummarizationSettingsException(key, CONFIG_INTERVAL_CLASS, "specifies an invalid MetricInterval class; aborting.");
            }

            LastResummarization last = Query.from(LastResummarization.class).using(database).where("key = ?", key).first();
            if (last == null) {
                last = new LastResummarization();
                last.getState().setDatabase(database);
                last.setKey(key);
            }

            // Ensure we're only running once per day. . .
            if (last.getRunDate() != null && last.getRunDate().isAfter(new DateTime().minusDays(1))) {
                throw new ResummarizationSettingsException(key, null, "last ran on " + last.getRunDate().toString("yyyy/MM/dd HH:mm:ss") + ", aborting.");
            }

            // Set up the new date range
            DateTime startDate = last.getEndDate();
            DateTime endDate = new DateTime().dayOfMonth().roundFloorCopy().minusDays(beforeDays);
            last.setRunDate(new DateTime());
            last.setStartDate(startDate);
            last.setEndDate(endDate);
            last.saveImmediately();
            for (ObjectField field : fields) {
                LOGGER.info("Submitting Metric Resummarization task : "
                        + "Database: " + database.getName() + ", "
                        + "Field: " + (field != null ? field.getInternalName() : null) + ", "
                        + "Interval: " + interval.getClass().getName() + ", "
                        + "Start Date: " + startDate + ", "
                        + "End Date: " + endDate);
                Task task = Metric.Static.submitResummarizeAllBetweenTask(database, field.getParentType(), field, interval, startDate, endDate, 1, "Periodic Metric Resummarization", key + " (" + field.getUniqueName() + ")");
                do {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                    if (!task.isRunning()) {
                        break;
                    }
                } while (parentTask == null || parentTask.shouldContinue());
            }
        }

        private static Set<ObjectField> resolveFieldSpecs(Database database, String key, String[] fieldSpecs) throws ResummarizationSettingsException {

            Set<ObjectField> fields = new HashSet<ObjectField>();
            for (String fieldSpec : fieldSpecs) {
                String[] parts = fieldSpec.split("/");
                if (parts.length == 1) {
                    // looking for a global field
                    if (fieldSpec.endsWith("*")) {
                        String fieldPrefix = fieldSpec.substring(0, fieldSpec.length() - 1);
                        for (ObjectField field : database.getEnvironment().getFields()) {
                            if (field.getInternalName().startsWith(fieldPrefix) && field.isMetric()) {
                                fields.add(field);
                            }
                        }

                    } else {
                        ObjectField field = database.getEnvironment().getField(fieldSpec);
                        if (field == null || !field.isMetric()) {
                            throw new ResummarizationSettingsException(key, CONFIG_FIELDS, ": " + fieldSpec + " specifies an invalid field.");
                        }
                        fields.add(field);
                    }

                } else if (parts.length == 2) {
                    // looking for a type field
                    String typeName = parts[0];
                    String typeFieldSpec = parts[1];
                    ObjectType type = database.getEnvironment().getTypeByName(typeName);
                    if (type == null) {
                        throw new ResummarizationSettingsException(key, CONFIG_FIELDS, ": " + fieldSpec + " specifies an invalid type.");
                    }

                    if (typeFieldSpec.endsWith("*")) {
                        String typeFieldPrefix = typeFieldSpec.substring(0, typeFieldSpec.length() - 1);
                        for (ObjectField field : type.getFields()) {
                            if (field.getInternalName().startsWith(typeFieldPrefix) && field.isMetric()) {
                                fields.add(field);
                            }
                        }

                    } else {
                        ObjectField field = type.getField(typeFieldSpec);
                        if (field == null || !field.isMetric()) {
                            throw new ResummarizationSettingsException(key, CONFIG_FIELDS, ": " + fieldSpec + " specifies an invalid field.");
                        }
                        fields.add(field);
                    }

                } else {
                    throw new ResummarizationSettingsException(key, CONFIG_FIELDS, ": " + fieldSpec + " specifies an invalid field.");
                }
            }
            return fields;
        }
    }
}
