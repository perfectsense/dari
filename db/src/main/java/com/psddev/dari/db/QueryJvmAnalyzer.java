package com.psddev.dari.db;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.asm.Opcodes;
import com.psddev.dari.util.sa.Jvm;
import com.psddev.dari.util.sa.JvmAnalyzer;
import com.psddev.dari.util.sa.JvmLogger;
import com.psddev.dari.util.sa.JvmMethodListener;
import com.psddev.dari.util.sa.JvmObject;
import com.psddev.dari.util.sa.JvmObjectListener;

public class QueryJvmAnalyzer extends JvmAnalyzer {

    @Override
    public void onStart() {
        Jvm jvm = getJvm();
        QueryMethodListener queryMethodListener = new QueryMethodListener();

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

            jvm.addMethodListener(queryMethod, queryMethodListener);
        }
    }

    @Override
    public void onStop() {
    }

    private class QueryMethodListener extends JvmMethodListener {

        @Override
        public void onInvocation(
                Method callingMethod,
                int callingLine,
                Method calledMethod,
                int calledLine,
                JvmObject calledObject,
                List<JvmObject> calledArguments,
                JvmObject returnedObject) {

            if ("count".equals(calledMethod.getName())) {
                returnedObject.addListener(new CountCompareListener(callingMethod, callingLine));
            }

            Object calledObjectResolved = calledObject.resolve();
            JvmLogger logger = getLogger();

            if (calledObjectResolved instanceof Query) {
                Query<?> query = (Query<?>) calledObjectResolved;

                try {
                    try {
                        SqlDatabase db = Database.Static.getFirst(SqlDatabase.class);

                        if (db != null) {
                            db.buildSelectStatement(query);
                        }

                    } catch (UnsupportedOperationException error) {
                        SolrDatabase db = Database.Static.getFirst(SolrDatabase.class);

                        if (db != null) {
                            db.buildQuery(query);
                        }
                    }

                } catch (Exception error) {
                    StringBuilder log = new StringBuilder();
                    String className = error.getClass().getName();
                    String message = error.getMessage();

                    log.append("Invalid query - ");
                    log.append(className);

                    if (!ObjectUtils.isBlank(message)) {
                        log.append(": ");
                        log.append(message);
                    }

                    if (!className.startsWith(Query.class.getName() + "$")) {
                        StringWriter st = new StringWriter();

                        error.printStackTrace(new PrintWriter(st));
                        log.append(st.toString());
                    }

                    logger.error(callingMethod, callingLine, log.toString());
                }

                for (Sorter sorter : query.getSorters()) {
                    String op = sorter.getOperator();

                    if (Sorter.ASCENDING_OPERATOR.equals(op)
                            || Sorter.DESCENDING_OPERATOR.equals(op)) {
                        List<Object> options = sorter.getOptions();

                        if (!options.isEmpty()) {
                            String field = options.get(0).toString();

                            if (!findComparison(query.getPredicate(), field)) {
                                logger.warn(callingMethod, callingLine, String.format(
                                        "Add [%s != missing] predicate when sorting by [%s] to improve performance.",
                                        field,
                                        field));
                            }
                        }
                    }
                }

            } else {
                logger.warn(callingMethod, callingLine, "Can't analyze this query.");
            }
        }

        private boolean findComparison(Predicate predicate, String field) {
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

    private class CountCompareListener extends JvmObjectListener {

        private final Method method;
        private final int line;

        public CountCompareListener(Method method, int line) {
            this.method = method;
            this.line = line;
        }

        @Override
        public void onBinary(int opcode, JvmObject other, boolean reverse) {
            switch (opcode) {
                case Opcodes.LCMP :
                case Opcodes.FCMPL :
                case Opcodes.FCMPG :
                case Opcodes.DCMPL :
                case Opcodes.DCMPG :
                    getLogger().warn(method, line, "Comparing against the result of [count] can be slow. Consider using [hasMoreThan] instead.");
                    break;

                default :
                    break;
            }
        }
    }
}
