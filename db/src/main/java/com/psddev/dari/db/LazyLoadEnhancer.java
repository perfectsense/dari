package com.psddev.dari.db;

import java.util.HashSet;
import java.util.Set;

import com.psddev.dari.util.ClassEnhancer;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.asm.AnnotationVisitor;
import com.psddev.dari.util.asm.ClassReader;
import com.psddev.dari.util.asm.FieldVisitor;
import com.psddev.dari.util.asm.MethodVisitor;
import com.psddev.dari.util.asm.Opcodes;
import com.psddev.dari.util.asm.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enables lazily loading fields that are expensive to initialize.
 * If the project uses Apache Maven to manage the build and inherits
 * from {@code com.psddev:dari-parent}, this enhancer is automatically
 * applied to all model classes.
 *
 * <p>Note that this is an optional performance optimization. It's always
 * safe not to enable it.</p>
 *
 * @see <a href="http://maven.apache.org/">Apache Maven</a>
 */
public class LazyLoadEnhancer extends ClassEnhancer {

    private static final String ANNOTATION_DESCRIPTOR = Type.getDescriptor(LazyLoad.class);
    private static final Logger LOGGER = LoggerFactory.getLogger(LazyLoadEnhancer.class);

    private boolean missingClasses;
    private String enhancedClassName;
    private boolean alreadyEnhanced;
    private final Set<String> transientFields = new HashSet<>();
    private final Set<String> recordableFields = new HashSet<>();

    // --- ClassEnhancer support ---

    // Returns the class associated with the given className
    // only if it's compatible with Recordable.
    private Class<?> findRecordableClass(String className) {
        Class<?> objectClass = ObjectUtils.getClassByName(className.replace('/', '.'));

        if (objectClass == null) {
            LOGGER.warn("Can't find [{}] referenced by [{}]!", className, enhancedClassName);
            missingClasses = true;
            return null;

        } else if (Recordable.class.isAssignableFrom(objectClass)) {
            return objectClass;

        } else {
            return null;
        }
    }

    @Override
    public boolean canEnhance(ClassReader reader) {
        enhancedClassName = reader.getClassName();

        return (!enhancedClassName.startsWith("com/psddev/dari/")
                || enhancedClassName.startsWith("com/psddev/dari/h2/"))
                && findRecordableClass(enhancedClassName) != null;
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
        if (!alreadyEnhanced && desc.equals(ANNOTATION_DESCRIPTOR)) {
            alreadyEnhanced = true;
        }

        return super.visitAnnotation(desc, visible);
    }

    @Override
    public FieldVisitor visitField(
            int access,
            String name,
            String desc,
            String signature,
            Object value) {

        if ((access & Opcodes.ACC_TRANSIENT) != 0) {
            transientFields.add(name);

        } else {
            Class<?> objectClass = findRecordableClass(Type.getType(desc).getClassName());

            if (objectClass != null) {
                Recordable.Embedded embedded = objectClass.getAnnotation(Recordable.Embedded.class);

                if (embedded == null || !embedded.value()) {
                    recordableFields.add(name);
                }
            }
        }

        return super.visitField(access, name, desc, signature, value);
    }

    @Override
    public MethodVisitor visitMethod(
            int access,
            String name,
            String desc,
            String signature,
            String[] exceptions) {

        MethodVisitor visitor = super.visitMethod(access, name, desc, signature, exceptions);

        if (alreadyEnhanced) {
            return visitor;

        } else {
            return new MethodVisitor(Opcodes.ASM5, visitor) {
                @Override
                public void visitFieldInsn(int opcode, String owner, String name, String desc) {
                    if (!transientFields.contains(name)
                            && !name.startsWith("this$")) {
                        if (opcode == Opcodes.GETFIELD) {
                            if (recordableFields.contains(name)) {
                                visitInsn(Opcodes.DUP);
                                visitMethodInsn(Opcodes.INVOKEINTERFACE, "com/psddev/dari/db/Recordable", "getState", "()Lcom/psddev/dari/db/State;", true);
                                visitInsn(Opcodes.DUP);
                                visitLdcInsn(name);
                                visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/psddev/dari/db/State", "beforeFieldGet", "(Ljava/lang/String;)V", false);
                                visitLdcInsn(name);
                                visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/psddev/dari/db/State", "resolveReference", "(Ljava/lang/String;)V", false);

                            } else {
                                visitInsn(Opcodes.DUP);
                                visitMethodInsn(Opcodes.INVOKEINTERFACE, "com/psddev/dari/db/Recordable", "getState", "()Lcom/psddev/dari/db/State;", true);
                                visitLdcInsn(name);
                                visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/psddev/dari/db/State", "beforeFieldGet", "(Ljava/lang/String;)V", false);
                            }

                        } else if (opcode == Opcodes.PUTFIELD
                                && recordableFields.contains(name)) {
                            visitInsn(Opcodes.SWAP);
                            visitInsn(Opcodes.DUP);
                            visitMethodInsn(Opcodes.INVOKEINTERFACE, "com/psddev/dari/db/Recordable", "getState", "()Lcom/psddev/dari/db/State;", true);
                            visitLdcInsn(name);
                            visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/psddev/dari/db/State", "resolveReference", "(Ljava/lang/String;)V", false);
                            visitInsn(Opcodes.SWAP);
                        }
                    }

                    super.visitFieldInsn(opcode, owner, name, desc);
                }
            };
        }
    }

    @Override
    public void visitEnd() {
        if (!missingClasses && !alreadyEnhanced) {
            AnnotationVisitor annotation = super.visitAnnotation(ANNOTATION_DESCRIPTOR, true);

            annotation.visitEnd();
        }

        super.visitEnd();
    }
}
