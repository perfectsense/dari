package com.psddev.dari.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

import javax.tools.JavaFileObject;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

/**
 * Enhances an existing class by manipulating the Java bytecode.
 * Subclasses should define how that's done by overriding the various
 * {@code visit*} methods. See the documentation on the
 * <a href="http://asm.ow2.org/">ASM</a> for more details.
 */
public abstract class ClassEnhancer extends ClassVisitor {

    protected ClassEnhancer() {
        super(Opcodes.ASM5);
    }

    /** Returns the delegate. */
    public ClassVisitor getDelegate() {
        return this.cv;
    }

    /** Sets the delegate. */
    public void setDelegate(ClassVisitor delegate) {
        this.cv = delegate;
    }

    /**
     * Returns {@code true} if this enhancer can enhance the class
     * represented by the given {@code reader}.
     */
    public boolean canEnhance(ClassReader reader) {
        return true;
    }

    /** {@linkplain ClassEnhancer Class enhancer} utility methods. */
    public static final class Static {

        /**
         * Enhances the given {@code bytecode} with the instances of
         * the given {@code enhancerClasses}.
         *
         * @return {@code null} if the given {@code bytecode} didn't
         * change.
         */
        public static byte[] enhance(
                byte[] bytecode,
                Iterable<Class<? extends ClassEnhancer>> enhancerClasses) {

            ClassReader reader = new ClassReader(bytecode);
            ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_FRAMES);
            ClassVisitor delegate = writer;

            for (Class<? extends ClassEnhancer> enhancerClass : enhancerClasses) {
                ClassEnhancer enhancer = TypeDefinition.getInstance(enhancerClass).newInstance();
                enhancer.setDelegate(delegate);
                if (enhancer.canEnhance(reader)) {
                    delegate = enhancer;
                }
            }

            if (writer == delegate) {
                return null;

            } else {
                reader.accept(delegate, ClassReader.SKIP_FRAMES);
                return writer.toByteArray();
            }
        }

        /**
         * Enhances all class files found within the given array of
         * {@code paths} using the instances of all enhancer classes
         * found in the current class loader.
         */
        public static void main(String[] paths) throws IOException {
            Set<Class<? extends ClassEnhancer>> enhancerClasses = ClassFinder.findClasses(ClassEnhancer.class);
            System.out.println("Enhancers: " + enhancerClasses);

            int count = 0;
            for (String path : paths) {
                count += enhanceFile(enhancerClasses, new File(path));
            }
            System.out.println("Enhanced [" + count + "] files");
        }

        private static int enhanceFile(
                Set<Class<? extends ClassEnhancer>> enhancerClasses,
                File file)
                throws IOException {

            if (!file.exists()) {
                // Can't process a non-existent file.

            } else if (file.isDirectory()) {
                int count = 0;
                for (File child : file.listFiles()) {
                    count += enhanceFile(enhancerClasses, child);
                }
                return count;

            } else if (file.getName().endsWith(JavaFileObject.Kind.CLASS.extension)) {
                byte[] bytecode = IoUtils.toByteArray(file);
                bytecode = Static.enhance(bytecode, enhancerClasses);

                if (bytecode != null) {
                    OutputStream output = new FileOutputStream(file);

                    try {
                        output.write(bytecode);
                    } finally {
                        output.close();
                    }

                    return 1;
                }
            }

            return 0;
        }
    }
}
