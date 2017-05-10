package com.psddev.dari.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;

/** Debug servlet for inspecting all active {@linkplain Task tasks}. */
@SuppressWarnings("serial")
public class TaskDebugServlet extends DebugServlet {

    @Override
    public String getName() {
        return "Background Tasks";
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("task");
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        WebPageContext page = new WebPageContext(getServletContext(), request, response);
        if (page.isFormPost()) {
            String action = page.param(String.class, "action");
            String executorName = page.param(String.class, "executor");

            if (executorName != null) {
                TaskExecutor executor = TaskExecutor.Static.getInstance(executorName);
                if ("Pause All".equals(action)) {
                    executor.pauseTasks();
                } else if ("Resume All".equals(action)) {
                    executor.resumeTasks();
                } else if ("Stop All".equals(action)) {
                    executor.shutdown();
                }
            }

            page.redirect("");
            return;
        }

        new DebugFilter.PageWriter(page) { {
            startPage("Task Status");

            for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
                writeStart("div", "style", "position: relative;");

                    writeStart("h2").writeHtml(executor.getName()).writeEnd();

                    writeStart("form", "method", "post", "style", "position: absolute; right: 0; top: 0;");
                        writeElement("input",
                                "name", "executor",
                                "type", "hidden",
                                "value", executor.getName());
                        writeElement("input",
                                "class", "btn btn-small btn-warning",
                                "name", "action",
                                "type", "submit",
                                "value", "Pause All");
                        writeHtml(' ');
                        writeElement("input",
                                "class", "btn btn-small btn-success",
                                "name", "action",
                                "type", "submit",
                                "value", "Resume All");
                        writeHtml(' ');
                        writeElement("input",
                                "class", "btn btn-small btn-danger",
                                "name", "action",
                                "type", "submit",
                                "value", "Stop All");
                    writeEnd();

                    List<Object> tasks = executor.getTasks();
                    if (tasks.isEmpty()) {
                        writeStart("p", "class", "alert alert-info");
                            writeHtml("No tasks!");
                        writeEnd();

                    } else {
                        Map<AsyncQueue<?>, QueueTasks> queues = new CompactMap<AsyncQueue<?>, QueueTasks>();

                        writeStart("table", "class", "table table-condensed table-striped");
                            writeStart("thead");
                                writeStart("tr");
                                    writeStart("th").writeHtml("Name").writeEnd();
                                    writeStart("th", "colspan", 2).writeHtml("Progress").writeEnd();
                                    writeStart("th").writeHtml("Last Exception").writeEnd();
                                    writeStart("th").writeHtml("Run Count").writeEnd();
                                    writeStart("th").writeHtml("Last Run Time").writeEnd();
                                writeEnd();
                            writeEnd();

                            writeStart("tbody");
                                for (Object taskObject : tasks) {
                                    if (!(taskObject instanceof Task)) {
                                        continue;
                                    }

                                    Task task = (Task) taskObject;
                                    writeStart("tr");
                                        writeStart("td").writeHtml(task.getName()).writeEnd();

                                        writeStart("td");
                                            if (task.isPauseRequested()) {
                                                writeStart("span", "class", "label label-warning");
                                                    writeHtml("Paused");
                                                writeEnd();

                                            } else if (task.isRunning()) {
                                                writeStart("span", "class", "label label-success");
                                                    writeHtml("Running");
                                                writeEnd();

                                            } else {
                                                Future<?> future = task.getFuture();
                                                if (future instanceof ScheduledFuture) {
                                                    writeStart("span", "class", "label label-warning");
                                                        writeHtml("Scheduled");
                                                    writeEnd();

                                                } else {
                                                    writeStart("span", "class", "label label-important");
                                                        writeHtml("Stopped");
                                                    writeEnd();
                                                }
                                            }
                                        writeEnd();

                                        double runDuration = task.getRunDuration() / 1e3;
                                        if (runDuration < 0) {
                                            writeStart("td");
                                            writeEnd();

                                        } else {
                                            writeStart("td");
                                                String progress = task.getProgress();
                                                if (!ObjectUtils.isBlank(progress)) {
                                                    writeHtml(progress);
                                                    writeHtml("; ");
                                                }

                                                writeStart("strong").writeObject(runDuration).writeEnd();
                                                writeHtml(" s total");

                                                long index = task.getProgressIndex();
                                                if (index > 0) {
                                                    writeHtml("; ");
                                                    writeStart("strong").writeObject(index / runDuration).writeEnd();
                                                    writeHtml(" items/s");
                                                }

                                                if (task instanceof AsyncProducer) {
                                                    AsyncProducer<?> producer = (AsyncProducer<?>) task;
                                                    double count = producer.getProduceCount();
                                                    double produceDuration = producer.getProduceDuration() / 1e6;

                                                    writeHtml("; ");
                                                    writeStart("strong").writeObject(produceDuration / count).writeEnd();
                                                    writeHtml(" ms/produce");

                                                    AsyncQueue<?> queue = producer.getOutput();
                                                    getQueueTasks(queues, queue).producers.add(task);

                                                } else if (task instanceof AsyncConsumer) {
                                                    AsyncConsumer<?> consumer = (AsyncConsumer<?>) task;
                                                    double count = consumer.getConsumeCount();
                                                    double consumeDuration = consumer.getConsumeDuration() / 1e6;

                                                    writeHtml("; ");
                                                    writeStart("strong").writeObject(consumeDuration / count).writeEnd();
                                                    writeHtml(" ms/consume");

                                                    AsyncQueue<?> queue = consumer.getInput();
                                                    getQueueTasks(queues, queue).consumers.add(task);

                                                    if (task instanceof AsyncProcessor) {
                                                        queue = ((AsyncProcessor<?, ?>) task).getOutput();
                                                        getQueueTasks(queues, queue).producers.add(task);
                                                    }
                                                }
                                            writeEnd();
                                        }

                                        writeStart("td").writeHtmlOrDefault(task.getLastException(), "N/A").writeEnd();
                                        writeStart("td").writeObject(task.getRunCount()).writeEnd();
                                        writeStart("td").writeObject(task.getLastRunBegin() > -1 ? new DateTime(task.getLastRunBegin()).toString("YYYY-MM-dd hh:mm:ss a z") : "").writeEnd();
                                    writeEnd();
                                }
                            writeEnd();
                        writeEnd();

                        if (!queues.isEmpty()) {
                            writeStart("h3").writeHtml(executor.getName()).writeHtml(" Queues").writeEnd();

                            writeStart("table", "class", "table table-bordered table-condensed table-striped");
                                writeStart("thead");
                                    writeStart("tr");
                                        writeStart("th", "colspan", 4).writeHtml("Production").writeEnd();
                                        writeStart("th", "colspan", 3).writeHtml("Consumption").writeEnd();
                                    writeEnd();

                                    writeStart("tr");
                                        writeStart("th").writeHtml("Tasks").writeEnd();
                                        writeStart("th").writeHtml("Successes").writeEnd();
                                        writeStart("th").writeHtml("Failures").writeEnd();
                                        writeStart("th").writeHtml("Wait").writeEnd();

                                        writeStart("th").writeHtml("Tasks").writeEnd();
                                        writeStart("th").writeHtml("Successes").writeEnd();
                                        writeStart("th").writeHtml("Wait").writeEnd();
                                    writeEnd();
                                writeEnd();

                                writeStart("tbody");
                                    for (Map.Entry<AsyncQueue<?>, QueueTasks> entry : queues.entrySet()) {
                                        AsyncQueue<?> queue = entry.getKey();
                                        QueueTasks queueTasks = entry.getValue();
                                        long addSuccessCount = queue.getAddSuccessCount();
                                        long addFailureCount = queue.getAddFailureCount();
                                        long removeCount = queue.getRemoveCount();

                                        writeStart("tr");
                                            writeStart("td").writeObject(queueTasks.producers).writeEnd();
                                            writeStart("td").writeObject(addSuccessCount).writeEnd();
                                            writeStart("td").writeObject(addFailureCount).writeEnd();
                                            writeStart("td").writeStart("strong").writeObject(((double) queue.getAddWait()) / ((double) (addSuccessCount + addFailureCount)) / 1e6).writeEnd().writeHtml(" ms/item").writeEnd();

                                            writeStart("td").writeObject(queueTasks.consumers).writeEnd();
                                            writeStart("td").writeObject(removeCount).writeEnd();
                                            writeStart("td").writeStart("strong").writeObject(((double) queue.getRemoveWait()) / ((double) removeCount) / 1e6).writeEnd().writeHtml(" ms/item").writeEnd();
                                        writeEnd();
                                    }
                                writeEnd();
                            writeEnd();
                        writeEnd();
                    }
                }
            }

            endPage();
        } };
    }

    private static QueueTasks getQueueTasks(Map<AsyncQueue<?>, QueueTasks> queues, AsyncQueue<?> queue) {
        QueueTasks queueTasks = queues.get(queue);
        if (queueTasks == null) {
            queueTasks = new QueueTasks();
            queues.put(queue, queueTasks);
        }
        return queueTasks;
    }

    private static class QueueTasks {
        public final List<Task> producers = new ArrayList<Task>();
        public final List<Task> consumers = new ArrayList<Task>();
    }
}
