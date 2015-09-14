package com.psddev.dari.db;

import com.psddev.dari.util.ClassEnhancer;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.asm.Label;
import com.psddev.dari.util.asm.MethodVisitor;
import com.psddev.dari.util.asm.Opcodes;
import com.psddev.dari.util.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class QueryCommentEnhancer extends ClassEnhancer {

    private static final String CLASS_INTERNAL_NAME;
    private static final String COMMENT_METHOD_NAME;
    private static final String COMMENT_METHOD_DESC;
    private static final List<FactoryMethod> FACTORY_METHODS;

    static {
        Class<?> queryClass = Query.class;

        CLASS_INTERNAL_NAME = Type.getInternalName(queryClass);

        Method commentMethod;

        try {
            commentMethod = queryClass.getMethod("comment", String.class);

        } catch (NoSuchMethodException error) {
            throw new IllegalStateException(error);
        }

        COMMENT_METHOD_NAME = commentMethod.getName();
        COMMENT_METHOD_DESC = Type.getMethodDescriptor(commentMethod);

        FACTORY_METHODS = Arrays.stream(queryClass.getMethods())
                .filter(method -> Modifier.isStatic(method.getModifiers())
                        && queryClass.isAssignableFrom(method.getReturnType()))
                .map(FactoryMethod::new)
                .collect(Collectors.toList());
    }

    private String currentSource;

    @Override
    public void visitSource(String source, String debug) {
        super.visitSource(source, debug);

        currentSource = StringUtils.removeEnd(source, ".java");
    }

    @Override
    public MethodVisitor visitMethod(
            int access,
            String name,
            String desc,
            String signature,
            String[] exceptions) {

        MethodVisitor visitor = super.visitMethod(access, name, desc, signature, exceptions);

        return new MethodVisitor(Opcodes.ASM5, visitor) {

            private int currentLine;

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
                super.visitMethodInsn(opcode, owner, name, desc, itf);

                if (opcode == Opcodes.INVOKESTATIC
                        && CLASS_INTERNAL_NAME.equals(owner)
                        && FACTORY_METHODS.stream().anyMatch(method -> method.matches(name, desc))) {

                    super.visitLdcInsn(currentSource + ":" + currentLine);
                    super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, CLASS_INTERNAL_NAME, COMMENT_METHOD_NAME, COMMENT_METHOD_DESC, false);
                }
            }

            @Override
            public void visitLineNumber(int line, Label start) {
                super.visitLineNumber(line, start);

                currentLine = line;
            }
        };
    }

    private static class FactoryMethod {

        private final String name;
        private final String desc;

        public FactoryMethod(Method method) {
            this.name = method.getName();
            this.desc = Type.getMethodDescriptor(method);
        }

        public String getName() {
            return name;
        }

        public String getDesc() {
            return desc;
        }

        public boolean matches(String name, String desc) {
            return this.name.equals(name)
                    && this.desc.equals(desc);
        }
    }
}
