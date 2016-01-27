package com.psddev.dari.streamsupport;

import java.util.Collection;
import java.util.Map;

import com.psddev.dari.util.ClassEnhancer;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.asm.AnnotationVisitor;
import com.psddev.dari.util.asm.ClassReader;
import com.psddev.dari.util.asm.FieldVisitor;
import com.psddev.dari.util.asm.Handle;
import com.psddev.dari.util.asm.Label;
import com.psddev.dari.util.asm.MethodVisitor;
import com.psddev.dari.util.asm.Opcodes;
import com.psddev.dari.util.asm.Type;
import com.psddev.dari.util.asm.TypePath;

public class StreamSupportEnhancer extends ClassEnhancer {

    private static final String ANNOTATION_DESCRIPTOR = Type.getDescriptor(StreamSupportEnhanced.class);

    private boolean alreadyEnhanced;

    @Override
    public boolean canEnhance(ClassReader reader) {
        return true;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        if (interfaces != null) {
            String[] java7Interfaces = new String[interfaces.length];
            for (int j = 0; j < interfaces.length; j++) {
                java7Interfaces[j] = convertJava8StreamToStreamSupport(interfaces[j]);
            }
            interfaces = java7Interfaces;
        }

        signature = convertJava8StreamToStreamSupport(signature);

        super.visit(version, access, name, signature, superName, interfaces);
    }

    @Override
    public void visitSource(String source, String debug) {
        source = convertJava8StreamToStreamSupport(source);
        debug = convertJava8StreamToStreamSupport(debug);
        super.visitSource(source, debug);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {

        desc = convertJava8StreamToStreamSupport(desc);
        signature = convertJava8StreamToStreamSupport(signature);

        return super.visitField(access, name, desc, signature, value);
    }

    @Override
    public void visitOuterClass(String owner, String name, String desc) {
        owner = convertJava8StreamToStreamSupport(owner);
        desc = convertJava8StreamToStreamSupport(desc);
        super.visitOuterClass(owner, name, desc);
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
        if (!alreadyEnhanced && desc.equals(ANNOTATION_DESCRIPTOR)) {
            alreadyEnhanced = true;
        }
        desc = convertJava8StreamToStreamSupport(desc);
        return super.visitAnnotation(desc, visible);
    }

    @Override
    public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
        desc = convertJava8StreamToStreamSupport(desc);
        return super.visitTypeAnnotation(typeRef, typePath, desc, visible);
    }

    @Override
    public MethodVisitor visitMethod(
            int access,
            String name,
            String desc,
            String signature,
            String[] exceptions) {

        desc = convertJava8StreamToStreamSupport(desc);
        signature = convertJava8StreamToStreamSupport(signature);

        MethodVisitor visitor = super.visitMethod(access, name, desc, signature, exceptions);

        if (alreadyEnhanced) {
            return visitor;
        }

        return new MethodVisitor(Opcodes.ASM5, visitor) {
            @Override
            public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
                desc = convertJava8StreamToStreamSupport(desc);
                return super.visitAnnotation(desc, visible);
            }

            @Override
            public AnnotationVisitor visitInsnAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
                desc = convertJava8StreamToStreamSupport(desc);
                return super.visitInsnAnnotation(typeRef, typePath, desc, visible);
            }

            @Override
            public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs) {
                desc = convertJava8StreamToStreamSupport(desc);
                super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
            }

            @Override
            public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
                desc = convertJava8StreamToStreamSupport(desc);
                signature = convertJava8StreamToStreamSupport(signature);
                super.visitLocalVariable(name, desc, signature, start, end, index);
            }

            @Override
            public AnnotationVisitor visitLocalVariableAnnotation(int typeRef, TypePath typePath, Label[] start, Label[] end, int[] index, String desc, boolean visible) {
                desc = convertJava8StreamToStreamSupport(desc);
                return super.visitLocalVariableAnnotation(typeRef, typePath, start, end, index, desc, visible);
            }

            @Override
            public void visitMultiANewArrayInsn(String desc, int dims) {
                desc = convertJava8StreamToStreamSupport(desc);
                super.visitMultiANewArrayInsn(desc, dims);
            }

            @Override
            public AnnotationVisitor visitTryCatchAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
                desc = convertJava8StreamToStreamSupport(desc);
                return super.visitTryCatchAnnotation(typeRef, typePath, desc, visible);
            }

            @Override
            public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String desc, boolean visible) {
                desc = convertJava8StreamToStreamSupport(desc);
                return super.visitTypeAnnotation(typeRef, typePath, desc, visible);
            }

            @Override
            public void visitFieldInsn(int opcode, String owner, String name, String desc) {
                owner = convertJava8StreamToStreamSupport(owner);
                desc = convertJava8StreamToStreamSupport(desc);
                super.visitFieldInsn(opcode, owner, name, desc);
            }

            @Override
            public void visitParameter(String name, int access) {
                name = convertJava8StreamToStreamSupport(name);
                super.visitParameter(name, access);
            }

            @Override
            public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
                desc = convertJava8StreamToStreamSupport(desc);
                return super.visitParameterAnnotation(parameter, desc, visible);
            }

            @Override
            public void visitTypeInsn(int opcode, String type) {
                type = convertJava8StreamToStreamSupport(type);
                super.visitTypeInsn(opcode, type);
            }

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {

                Class klass = null;
                try {
                    klass = Class.forName(owner.replace("/", "."));
                } catch (ClassNotFoundException e) {
                    //Class not found
                } catch (NoClassDefFoundError error) {
                    super.visitMethodInsn(opcode, owner, name, desc, itf);
                    return;
                }

                boolean collection = klass != null && Collection.class.isAssignableFrom(klass);

                if (collection && name.equals("stream")) {
                    opcode = 184;
                    desc = "(Ljava/util/Collection;)Ljava8/util/stream/Stream;";
                    owner = "java8/util/stream/StreamSupport";
                    itf = false;

                    super.visitMethodInsn(opcode, owner, name, desc, itf);

                } else if (owner.equals("java/util/Arrays") && name.equals("stream")) {
                    super.visitMethodInsn(184, "java/util/Arrays", "asList", "([Ljava/lang/Object;)Ljava/util/List;", false);
                    super.visitMethodInsn(184, "java8/util/stream/StreamSupport", "stream", "(Ljava/util/Collection;)Ljava8/util/stream/Stream;", false);

                } else if (collection && name.equals("forEach")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", "forEach", "(Ljava/lang/Iterable;Ljava8/util/function/Consumer;)V", false);

                } else if (klass != null && Map.class.isAssignableFrom(klass) && name.equals("forEach")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", "mapForEach", "(Ljava/util/Map;Ljava8/util/function/BiConsumer;)V", false);

                } else if (collection && name.equals("removeIf") && desc.equals("(Ljava/util/function/Predicate;)Z")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", "removeIf", "(Ljava/lang/Iterable;Ljava8/util/function/Predicate;)Z", false);

                } else if (owner.equals("java/util/stream/Stream") && name.equals("of") && desc.equals("([Ljava/lang/Object;)Ljava/util/stream/Stream;")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", "of", "([Ljava/lang/Object;)Ljava8/util/stream/Stream;", false);

                } else if (owner.equals("java/util/regex/Pattern") && name.equals("splitAsStream") && desc.equals("(Ljava/lang/CharSequence;)Ljava/util/stream/Stream;")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", "splitAsStream", "(Ljava/util/regex/Pattern;Ljava/lang/CharSequence;)Ljava8/util/stream/Stream;", false);

                } else if (owner.equals("java/util/function/Function") && name.equals("identity") && desc.equals("()Ljava/util/function/Function;")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", "identity", "()Ljava8/util/function/Function;", false);

                } else if (owner.equals("java/util/stream/Stream") && name.equals("concat") && desc.equals("(Ljava/util/stream/Stream;Ljava/util/stream/Stream;)Ljava/util/stream/Stream;")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", "concat", "(Ljava8/util/stream/Stream;Ljava8/util/stream/Stream;)Ljava8/util/stream/Stream;", false);

                } else if (owner.equals("java/util/Comparator")
                                && (name.equals("comparing")
                                || name.equals("comparingInt")
                                || name.equals("comparingLong")
                                || name.equals("comparingDouble"))) {

                    super.visitMethodInsn(184, "java8/util/Comparators", name, convertJava8StreamToStreamSupport(desc), false);

                } else if (owner.equals("java/util/Comparator") && name.equals("thenComparing") && desc.equals("(Ljava/util/function/Function;)Ljava/util/Comparator;")) {
                    super.visitMethodInsn(184, "java8/util/Comparators", name, "(Ljava/util/Comparator;Ljava8/util/function/Function;)Ljava/util/Comparator;", false);

                } else if (owner.equals("java/util/stream/IntStream")
                                && (name.equals("builder")
                                || name.equals("empty")
                                || name.equals("of")
                                || name.equals("iterate")
                                || name.equals("generate")
                                || name.equals("range")
                                || name.equals("rangeClosed")
                                || name.equals("concat"))) {

                    super.visitMethodInsn(184, "java8/util/stream/IntStreams", name, convertJava8StreamToStreamSupport(desc), itf);

                } else if (owner.equals("java/util/List") && name.equals("sort") && desc.equals("(Ljava/util/Comparator;)V")) {
                    super.visitMethodInsn(184, "java8/util/Lists", name, "(Ljava/util/List;Ljava/util/Comparator;)V", false);

                } else if (owner.equals("java/lang/Class") && name.equals("getAnnotationsByType") && desc.equals("(Ljava/lang/Class;)[Ljava/lang/annotation/Annotation;")) {
                    super.visitMethodInsn(184, "com/psddev/dari/util/StreamSupportUtils", name, "(Ljava/lang/Class;Ljava/lang/Class;)[Ljava/lang/annotation/Annotation;", false);

                } else {

                    owner = convertJava8StreamToStreamSupport(owner);
                    desc = convertJava8StreamToStreamSupport(desc);

                    super.visitMethodInsn(opcode, owner, name, desc, itf);
                }

            }
        };
    }

    @Override
    public void visitEnd() {
        if (!alreadyEnhanced) {
            AnnotationVisitor annotation = super.visitAnnotation(ANNOTATION_DESCRIPTOR, true);

            annotation.visitEnd();
        }

        super.visitEnd();
    }

    private String convertJava8StreamToStreamSupport(String input) {
        if (!StringUtils.isBlank(input)) {
            if (input.contains("java/util/function/")) {
                input = input.replace("java/util/function/", "java8/util/function/");
            }

            if (input.contains("java/util/stream/")) {
                input = input.replace("java/util/stream/", "java8/util/stream/");
            }

            if (input.contains("java/util/Optional")) {
                input = input.replace("java/util/Optional", "java8/util/Optional");
            }
        }

        return input;
    }

}
