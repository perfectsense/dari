package com.psddev.dari.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Debug servlet for inspecting {@linkplain Settings global settings}. */
@DebugFilter.Path("settings")
public class SettingsDebugServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SettingsDebugServlet.class);

    // --- HttpServlet support ---

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Settings");

            writeStart("h2");
                writeHtml("Values");
            writeEnd();

            writeStart("table", "class", "table table-condensed table-striped");
                writeStart("thead");
                    writeStart("tr");
                        writeStart("th").writeHtml("Key").writeEnd();
                        writeStart("th").writeHtml("Value").writeEnd();
                        writeStart("th").writeHtml("Class").writeEnd();
                    writeEnd();
                writeEnd();

                writeStart("tbody");
                    for (Map.Entry<String, Object> entry : flatten(Settings.asMap()).entrySet()) {
                        String key = entry.getKey();
                        String keyLowered = key.toLowerCase(Locale.ENGLISH);
                        Object value = entry.getValue();

                        writeStart("tr");
                            writeStart("td").writeHtml(key).writeEnd();

                            writeStart("td");
                                if (keyLowered.contains("password")
                                        || keyLowered.contains("secret")) {
                                    writeStart("span", "class", "label label-warning").writeHtml("Hidden").writeEnd();
                                } else {
                                    writeHtml(value);
                                }
                            writeEnd();

                            writeStart("td");
                                writeHtml(value != null ? value.getClass().getName() : "N/A");
                            writeEnd();
                        writeEnd();
                    }
                writeEnd();
            writeEnd();

            List<Usage> usages = new ArrayList<Usage>();

            for (Class<?> objectClass : ClassFinder.Static.findClasses(Object.class)) {
                if (Settings.class.isAssignableFrom(objectClass)) {
                    continue;
                }

                InputStream classInput = SettingsDebugServlet.class.getResourceAsStream("/" + objectClass.getName().replace('.', File.separatorChar) + ".class");

                if (classInput != null) {
                    try {
                        new ClassReader(classInput).accept(
                                new CV(usages, objectClass.getName()),
                                ClassReader.SKIP_FRAMES);

                    } finally {
                        classInput.close();
                    }
                }
            }

            if (!usages.isEmpty()) {
                Collections.sort(usages);

                writeStart("h2");
                    writeHtml("Usages");
                writeEnd();

                writeStart("table", "class", "table table-condensed table-striped");
                    writeStart("thead");
                        writeStart("tr");
                            writeStart("th").writeHtml("Key").writeEnd();
                            writeStart("th").writeHtml("Default").writeEnd();
                            writeStart("th").writeHtml("Method").writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("tbody");
                        for (Usage usage : usages) {
                            usage.writeRowHtml(this);
                        }
                    writeEnd();
                writeEnd();
            }

            endPage();
        } };
    }

    private Map<String, Object> flatten(Map<String, Object> map) {
        Map<String, Object> flattened = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        collectFlattenedValues(flattened, null, map);
        return flattened;
    }

    private void collectFlattenedValues(Map<String, Object> flattened, String key, Object value) {
        if (value instanceof Map) {
            String prefix = key == null ? "" : key + "/";
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                collectFlattenedValues(flattened, prefix + entry.getKey(), entry.getValue());
            }

        } else if (value instanceof DataSource) {
            try {
                Map<String, Object> map = new TreeMap<String, Object>();

                for (PropertyDescriptor desc : Introspector.getBeanInfo(value.getClass()).getPropertyDescriptors()) {
                    String name = desc.getName();
                    Throwable error = null;

                    try {
                        Method getter = desc.getReadMethod();
                        Method setter = desc.getWriteMethod();

                        if (getter != null && setter != null) {
                            getter.setAccessible(true);
                            map.put(name, getter.invoke(value));
                        }

                    } catch (IllegalAccessException e) {
                        error = e;
                    } catch (InvocationTargetException e) {
                        error = e.getCause();
                    } catch (RuntimeException e) {
                        error = e;
                    }

                    if (error != null) {
                        LOGGER.debug(String.format(
                                "Can't read [%s] from an instance of [%s] stored in [%s]!",
                                name, value.getClass(), key),
                                error);
                    }
                }

                collectFlattenedValues(flattened, key, map);

            } catch (IntrospectionException error) {
                flattened.put(key, value);
            }

        } else {
            flattened.put(key, value);
        }
    }

    private static class Usage implements Comparable<Usage> {

        private final String className;
        private final String methodName;
        private final int lineNumber;
        private final String key;
        private final Object defaultValue;

        public Usage(String className, String methodName, int lineNumber, String key, Object defaultValue) {
            this.className = className;
            this.methodName = methodName;
            this.lineNumber = lineNumber;
            this.key = key;
            this.defaultValue = defaultValue;
        }

        public void writeRowHtml(DebugFilter.PageWriter writer) throws IOException {
            File source = CodeUtils.getSource(className);

            writer.writeStart("tr");
                writer.writeStart("td");
                    writer.writeHtml(key);
                writer.writeEnd();

                writer.writeStart("td");
                    writer.writeHtml(defaultValue);
                writer.writeEnd();

                writer.writeStart("td");
                    if (source != null) {
                        writer.writeStart("a",
                                "target", "_blank",
                                "href", DebugFilter.Static.getServletPath(writer.page.getRequest(), "code",
                                        "file", source,
                                        "line", lineNumber));
                            writer.writeHtml(className);
                            writer.writeHtml('.');
                            writer.writeHtml(methodName);
                            writer.writeHtml(':');
                            writer.writeHtml(lineNumber);
                        writer.writeEnd();

                    } else {
                        writer.writeHtml(className);
                        writer.writeHtml('.');
                        writer.writeHtml(methodName);
                        writer.writeHtml(':');
                        writer.writeHtml(lineNumber);
                    }
                writer.writeEnd();
            writer.writeEnd();
        }

        @Override
        public int compareTo(Usage other) {
            return ObjectUtils.compare(key, other.key, true);
        }
    }

    private static class CV extends ClassVisitor {

        private final List<Usage> usages;
        private final String className;

        public CV(List<Usage> usages, String className) {
            super(Opcodes.ASM5);

            this.usages = usages;
            this.className = className;
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            return null;
        }

        @Override
        public void visitAttribute(Attribute attr) {
        }

        @Override
        public void visitEnd() {
        }

        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            return null;
        }

        @Override
        public void visitInnerClass(String name, String outerName, String innerName, int access) {
        }

        @Override
        public MethodVisitor visitMethod(
                int access,
                String name,
                String desc,
                String signature,
                String[] exceptions) {

            return new MV(usages, className, name);
        }

        @Override
        public void visitOuterClass(String owner, String name, String desc) {
        }

        @Override
        public void visitSource(String source, String debug) {
        }
    }

    private static class MV extends MethodVisitor {

        private final List<Usage> usages;
        private final String className;
        private final String methodName;
        private final List<Object> constants = new ArrayList<Object>();
        private int lastLineNumber;
        private boolean found;

        public MV(
                List<Usage> usages,
                String className,
                String methodName) {

            super(Opcodes.ASM5);

            this.usages = usages;
            this.className = className;
            this.methodName = methodName;
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            return null;
        }

        @Override
        public AnnotationVisitor visitAnnotationDefault() {
            return null;
        }

        @Override
        public void visitAttribute(Attribute attr) {
        }

        @Override
        public void visitCode() {
        }

        @Override
        public void visitEnd() {
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String desc) {
        }

        @Override
        public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack) {
        }

        @Override
        public void visitIincInsn(int var, int increment) {
        }

        @Override
        public void visitInsn(int opcode) {
        }

        @Override
        public void visitIntInsn(int opcode, int operand) {
        }

        @Override
        public void visitJumpInsn(int opcode, Label label) {
        }

        @Override
        public void visitLabel(Label label) {
        }

        @Override
        public void visitLdcInsn(Object cst) {
            if (cst instanceof Type) {
                cst = ObjectUtils.getClassByName(((Type) cst).getClassName());

                if (cst == null) {
                    return;
                }
            }

            constants.add(cst);
        }

        @Override
        public void visitLineNumber(int line, Label start) {
            lastLineNumber = line;
        }

        @Override
        public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
        }

        @Override
        public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels) {
        }

        @Override
        public void visitMaxs(int maxStack, int maxLocals) {
        }

        private Object findLastConstant(Class<?> constantClass) {
            if (constantClass != null) {
                for (int i = constants.size() - 1; i >= 0; -- i) {
                    Object constant = constants.get(i);

                    if (constantClass.isInstance(constant)) {
                        constants.remove(i);
                        return constant;
                    }
                }
            }

            return null;
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc) {
            if (!found
                    && opcode == Opcodes.INVOKESTATIC
                    && owner.equals("com/psddev/dari/util/Settings")
                    && name.startsWith("get")) {

                found = true;
                Object defaultValue = null;

                if (name.equals("getOrError")) {
                    findLastConstant(String.class);

                } else if (desc.endsWith(";Ljava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;")) {
                    Class<?> valueClass = (Class<?>) findLastConstant(Class.class);
                    defaultValue = findLastConstant(valueClass);
                }

                usages.add(new Usage(
                        className,
                        methodName,
                        lastLineNumber,
                        (String) findLastConstant(String.class),
                        defaultValue));
            }
        }

        @Override
        public void visitMultiANewArrayInsn(String desc, int dims) {
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
            return null;
        }

        @Override
        public void visitTableSwitchInsn(int min, int max, Label dflt, Label[] labels) {
        }

        @Override
        public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {
        }

        @Override
        public void visitTypeInsn(int opcode, String type) {
        }

        @Override
        public void visitVarInsn(int opcode, int var) {
        }
    }
}
