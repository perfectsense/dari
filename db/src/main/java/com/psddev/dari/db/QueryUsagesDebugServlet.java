package com.psddev.dari.db;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.ClassFinder;
import com.psddev.dari.util.CodeUtils;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.WebPageContext;
import com.psddev.dari.util.asm.Opcodes;
import com.psddev.dari.util.sa.Jvm;
import com.psddev.dari.util.sa.JvmMethodListener;
import com.psddev.dari.util.sa.JvmObject;
import com.psddev.dari.util.sa.JvmObjectListener;

@DebugFilter.Path("db-query-usages")
public class QueryUsagesDebugServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        @SuppressWarnings("resource")
        WebPageContext page = new WebPageContext(getServletContext(), request, response);
        @SuppressWarnings("resource")
        DebugFilter.PageWriter writer = new DebugFilter.PageWriter(getServletContext(), request, response);
        Jvm queryJvm = new Jvm();
        List<Invocation> queryInvocations = new ArrayList<Invocation>();
        Map<Method, List<Invocation>> callersByMethod = new CompactMap<Method, List<Invocation>>();
        QueryMethodListener queryMethodListener = new QueryMethodListener(request, queryInvocations, callersByMethod);

        for (Method queryMethod : Query.class.getDeclaredMethods()) {
            if (Query.class.equals(queryMethod.getReturnType())) {
                continue;
            }

            int mods = queryMethod.getModifiers();

            if (Modifier.isStatic(mods)
                    || !Modifier.isPublic(mods)) {
                continue;
            }

            String name = queryMethod.getName();

            if (name.startsWith("get")
                    || name.startsWith("is")
                    || name.startsWith("set")
                    || name.startsWith("create")
                    || name.startsWith("find")
                    || name.startsWith("map")) {
                continue;
            }

            queryJvm.addMethodListener(queryMethod, queryMethodListener);
        }

        List<Class<?>> allClasses = new ArrayList<Class<?>>();

        for (Class<?> c : ClassFinder.findClasses(Object.class)) {
            String cn = c.getName();

            if (!cn.startsWith("com.psddev.")) {
                allClasses.add(c);
            }
        }

        Collections.sort(allClasses, new Comparator<Class<?>>() {

            @Override
            public int compare(Class<?> x, Class<?> y) {
                return x.getName().compareTo(y.getName());
            }
        });

        Class<?> selectedClass = ObjectUtils.getClassByName(page.param(String.class, "c"));

        for (Class<?> c : allClasses) {
            if (selectedClass == null || c.equals(selectedClass)) {
                try {
                    queryJvm.analyze(c);

                } catch (Exception error) {
                    System.out.println(c);
                    error.printStackTrace();
                }
            }
        }

        for (int i = 0; i < 5 && !callersByMethod.isEmpty(); ++ i) {
            Jvm callingJvm = new Jvm();
            Map<Method, List<Invocation>> newCallersByMethod = new CompactMap<Method, List<Invocation>>();

            for (Map.Entry<Method, List<Invocation>> entry : callersByMethod.entrySet()) {
                callingJvm.addMethodListener(
                        entry.getKey(),
                        new StandardMethodListener(request, newCallersByMethod, entry.getValue()));
            }

            for (Class<?> c : allClasses) {
                try {
                    callingJvm.analyze(c);

                } catch (Exception error) {
                    System.out.println(c);
                    error.printStackTrace();
                }
            }

            callersByMethod = newCallersByMethod;
        }

        writer.startPage();
            writer.writeStart("form",
                    "method", "get",
                    "action", page.url(null));
                writer.writeStart("select", "name", "c");
                    writer.writeStart("option", "value", "");
                        writer.writeHtml("All Classes");
                    writer.writeEnd();

                    for (Class<?> c : allClasses) {
                        String cn = c.getName();

                        writer.writeStart("option", "value", cn);
                            writer.writeHtml(cn);
                        writer.writeEnd();
                    }
                writer.writeEnd();

                writer.writeStart("button", "class", "btn");
                    writer.writeHtml("View");
                writer.writeEnd();
            writer.writeEnd();

            for (Invocation invocation : queryInvocations) {
                writer.writeStart("div", "style", "margin-bottom: 40px;");
                    writer.writeObject(invocation);
                writer.writeEnd();
            }
        writer.endPage();
    }

    private static class Invocation implements HtmlObject {

        private final boolean query;
        private final HttpServletRequest request;
        private final Method callingMethod;
        private final Method calledMethod;
        private final int calledLine;
        private final Object calledObjectResolved;
        private final String stringified;
        private final List<Invocation> callers;
        private boolean countCompare;

        public Invocation(
                boolean query,
                HttpServletRequest request,
                Method callingMethod,
                Method calledMethod,
                int calledLine,
                JvmObject calledObject,
                List<JvmObject> calledArguments,
                JvmObject returnedObject,
                List<Invocation> callers) {

            this.query = query;
            this.request = request;
            this.callingMethod = callingMethod;
            this.calledMethod = calledMethod;
            this.calledLine = calledLine;
            this.calledObjectResolved = calledObject.resolve();
            this.stringified = String.valueOf(ObjectUtils.firstNonNull(returnedObject, calledObject));
            this.callers = callers;
        }

        public void setCountCompare(boolean countCompare) {
            this.countCompare = countCompare;
        }

        @Override
        public void format(HtmlWriter writer) throws IOException {
            String callingClassName = callingMethod.getDeclaringClass().getName();
            File source = CodeUtils.getSource(callingClassName);

            if (source != null) {
                writer.writeStart("a",
                        "target", "_blank",
                        "href", DebugFilter.Static.getServletPath(request, "code",
                                "file", source,
                                "line", calledLine));
            }

            writer.writeHtml(callingClassName);
            writer.writeHtml('.');
            writer.writeHtml(callingMethod.getName());
            writer.writeHtml(':');
            writer.writeHtml(calledLine);

            if (source != null) {
                writer.writeEnd();
            }

            writer.writeTag("br");

            writer.writeStart("pre");
                writer.writeHtml(stringified);
            writer.writeEnd();

            if (query) {
                if (calledObjectResolved instanceof Query) {
                    Query<?> query = (Query<?>) calledObjectResolved;

                    try {
                        try {
                            Database.Static.getFirst(SqlDatabase.class).buildSelectStatement(query);

                            writer.writeStart("div", "class", "alert alert-info");
                                writer.writeHtml("SQL");
                            writer.writeEnd();

                        } catch (UnsupportedOperationException error) {
                            Database.Static.getFirst(SolrDatabase.class).buildQuery(query);

                            writer.writeStart("div", "class", "alert alert-info");
                                writer.writeHtml("Solr");
                            writer.writeEnd();
                        }

                    } catch (Exception error) {
                        String message = error.getMessage();

                        writer.writeStart("div", "class", "alert alert-error");
                            writer.writeHtml("Invalid query - ");
                            writer.writeHtml(error.getClass().getName());

                            if (!ObjectUtils.isBlank(message)) {
                                writer.writeHtml(": ");
                                writer.writeHtml(message);
                            }
                        writer.writeEnd();
                    }

                    for (Sorter sorter : query.getSorters()) {
                        String op = sorter.getOperator();

                        if (Sorter.ASCENDING_OPERATOR.equals(op)
                                || Sorter.DESCENDING_OPERATOR.equals(op)) {
                            List<Object> options = sorter.getOptions();

                            if (!options.isEmpty()) {
                                String field = options.get(0).toString();

                                if (!findComparison(query.getPredicate(), field)) {
                                    writer.writeStart("div", "class", "alert alert-warning");
                                        writer.writeHtml("For better performance, add ");
                                        writer.writeStart("code");
                                            writer.writeHtml(field);
                                            writer.writeHtml(" != missing");
                                        writer.writeEnd();
                                        writer.writeHtml(" predicate when sorting by ");
                                        writer.writeStart("code");
                                            writer.writeHtml(field);
                                        writer.writeEnd();
                                        writer.writeHtml(" in ");
                                        writer.writeStart("code");
                                            writer.writeHtml(op);
                                        writer.writeEnd();
                                        writer.writeHtml(" order.");
                                    writer.writeEnd();
                                }
                            }
                        }
                    }

                    if (query.getPredicate() == null
                            && "selectAll".equals(calledMethod.getName())) {
                        /*
                        writer.writeStart("div", "class", "alert alert-warning");
                            writer.writeHtml("Calling ");
                            writer.writeStart("code");
                                writer.writeHtml("selectAll");
                            writer.writeEnd();
                            writer.writeHtml(" without a predicate could return a lot of results. Considering using ");
                            writer.writeStart("code");
                                writer.writeHtml("iterable");
                            writer.writeEnd();
                            writer.writeHtml(" instead.");
                        writer.writeEnd();
                        */
                    }

                } else {
                    writer.writeStart("div", "class", "alert alert-warning");
                        writer.writeHtml("Can't analyze this query.");
                    writer.writeEnd();
                }

                if (countCompare) {
                    writer.writeStart("div", "class", "alert alert-warning");
                        writer.writeHtml("Comparing against the result of ");
                        writer.writeStart("code");
                            writer.writeHtml("count");
                        writer.writeEnd();
                        writer.writeHtml(" can be slow. Consider using ");
                        writer.writeStart("code");
                            writer.writeHtml("hasMoreThan");
                        writer.writeEnd();
                        writer.writeHtml(" instead.");
                    writer.writeEnd();
                }
            }

            if (!callers.isEmpty()) {
                writer.writeStart("ul");
                    for (Invocation invocation : callers) {
                        writer.writeStart("li");
                            writer.writeObject(invocation);
                        writer.writeEnd();
                    }
                writer.writeEnd();
            }
        }

        private static boolean findComparison(Predicate predicate, String field) {
            if (predicate instanceof CompoundPredicate) {
                CompoundPredicate compound = (CompoundPredicate) predicate;

                if (PredicateParser.AND_OPERATOR.equals(compound.getOperator())) {
                    for (Predicate child : compound.getChildren()) {
                        if (findComparison(child, field)) {
                            return true;
                        }
                    }
                }

            } else if (predicate instanceof ComparisonPredicate) {
                ComparisonPredicate comparison = (ComparisonPredicate) predicate;

                if (comparison.getKey().endsWith(field)) {
                    return true;
                }
            }

            return false;
        }
    }

    private static class QueryMethodListener extends JvmMethodListener {

        private final HttpServletRequest request;
        private final List<Invocation> queryInvocations;
        private final Map<Method, List<Invocation>> callersByMethod;

        public QueryMethodListener(
                HttpServletRequest request,
                List<Invocation> queryInvocations,
                Map<Method, List<Invocation>> callersByMethod) {

            this.request = request;
            this.queryInvocations = queryInvocations;
            this.callersByMethod = callersByMethod;
        }

        @Override
        public void onInvocation(
                Method callingMethod,
                int callingLine,
                Method calledMethod,
                int calledLine,
                JvmObject calledObject,
                List<JvmObject> calledArguments,
                JvmObject returnedObject) {

            List<Invocation> callers = new ArrayList<Invocation>();
            Invocation invocation = new Invocation(
                    true,
                    request,
                    callingMethod,
                    calledMethod,
                    calledLine,
                    calledObject,
                    calledArguments,
                    returnedObject,
                    callers);

            queryInvocations.add(invocation);
            callersByMethod.put(callingMethod, callers);

            if ("count".equals(calledMethod.getName())) {
                returnedObject.addListener(new CountCompareListener(invocation));
            }
        }
    }

    private static class CountCompareListener extends JvmObjectListener {

        private final Invocation invocation;

        public CountCompareListener(Invocation invocation) {
            this.invocation = invocation;
        }

        @Override
        public void onBinary(int opcode, JvmObject other, boolean reverse) {
            switch (opcode) {
                case Opcodes.LCMP :
                case Opcodes.FCMPL :
                case Opcodes.FCMPG :
                case Opcodes.DCMPL :
                case Opcodes.DCMPG :
                    invocation.setCountCompare(true);
                    break;

                default :
                    break;
            }
        }
    }

    private static class StandardMethodListener extends JvmMethodListener {

        private final HttpServletRequest request;
        private final Map<Method, List<Invocation>> newCallersByMethod;
        private final List<Invocation> callers;

        public StandardMethodListener(
                HttpServletRequest request,
                Map<Method, List<Invocation>> newCallersByMethod,
                List<Invocation> callers) {

            this.request = request;
            this.newCallersByMethod = newCallersByMethod;
            this.callers = callers;
        }

        @Override
        public void onInvocation(
                Method callingMethod,
                int callingLine,
                Method calledMethod,
                int calledLine,
                JvmObject calledObject,
                List<JvmObject> calledArguments,
                JvmObject returnedObject) {

            List<Invocation> newCallers = newCallersByMethod.get(callingMethod);

            if (newCallers == null) {
                newCallers = new ArrayList<Invocation>();
                newCallersByMethod.put(callingMethod, newCallers);
            }

            Invocation invocation = new Invocation(
                    false,
                    request,
                    callingMethod,
                    calledMethod,
                    calledLine,
                    calledObject,
                    calledArguments,
                    returnedObject,
                    newCallers);

            callers.add(invocation);
        }
    }
}
