package com.psddev.dari.util.sa;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;

class JvmRunner extends MethodVisitor {

    private int callingMethodLine;
    private int instructionIndex;
    private int lastLine;
    private final Map<Integer, JvmObject> locals;
    private final JvmMethodVisitor parent;
    private final Deque<JvmObject> stack;

    public JvmRunner(JvmMethodVisitor parent) {
        super(Opcodes.ASM5);

        this.locals = new CompactMap<Integer, JvmObject>();
        this.parent = parent;
        this.stack = new ArrayDeque<JvmObject>();
    }

    public JvmRunner(JvmRunner runner, int instructionIndex) {
        super(Opcodes.ASM5);

        this.instructionIndex = instructionIndex;
        this.lastLine = runner.lastLine;
        this.locals = runner.locals;
        this.parent = runner.parent;
        this.stack = runner.stack;
    }

    public void run() {
        for (int size = parent.instructions.size();
                instructionIndex >= 0 && instructionIndex < size;
                ++ instructionIndex) {

            parent.instructions.get(instructionIndex).accept(this);
        }
    }

    public static Class<?> typeToClass(Type type) {
        String className = type.getClassName();
        int dimensions = 0;

        while (className.endsWith("[]")) {
            className = className.substring(0, className.length() - 2);
            ++ dimensions;
        }

        Class<?> objectClass = ObjectUtils.getClassByName(className);

        return dimensions > 0
                ? Array.newInstance(objectClass, new int[dimensions]).getClass()
                : objectClass;
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
        switch (opcode) {
            case Opcodes.GETSTATIC :
                stack.push(new JvmObjectField(Type.getType(desc), Type.getObjectType(owner), name));
                break;

            case Opcodes.PUTSTATIC :
                stack.pop();
                break;

            case Opcodes.GETFIELD :
                stack.pop();
                stack.push(new JvmObjectField(Type.getType(desc), null, name));
                break;

            case Opcodes.PUTFIELD :
                stack.pop();
                stack.pop();
                break;

            default :
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack) {
    }

    @Override
    public void visitIincInsn(int var, int increment) {
    }

    @Override
    public void visitInsn(int opcode) {
        switch (opcode) {
            case Opcodes.NOP :
                break;

            case Opcodes.ACONST_NULL :
                stack.push(new JvmObjectConstant(null));
                break;

            case Opcodes.ICONST_M1 :
                stack.push(new JvmObjectConstant(-1));
                break;

            case Opcodes.ICONST_0 :
                stack.push(new JvmObjectConstant(0));
                break;

            case Opcodes.ICONST_1 :
                stack.push(new JvmObjectConstant(1));
                break;

            case Opcodes.ICONST_2 :
                stack.push(new JvmObjectConstant(2));
                break;

            case Opcodes.ICONST_3 :
                stack.push(new JvmObjectConstant(3));
                break;

            case Opcodes.ICONST_4 :
                stack.push(new JvmObjectConstant(4));
                break;

            case Opcodes.ICONST_5 :
                stack.push(new JvmObjectConstant(5));
                break;

            case Opcodes.LCONST_0 :
                stack.push(new JvmObjectConstant(0L));
                break;

            case Opcodes.LCONST_1 :
                stack.push(new JvmObjectConstant(1L));
                break;

            case Opcodes.FCONST_0 :
                stack.push(new JvmObjectConstant(0.0f));
                break;

            case Opcodes.FCONST_1 :
                stack.push(new JvmObjectConstant(1.0f));
                break;

            case Opcodes.FCONST_2 :
                stack.push(new JvmObjectConstant(2.0f));
                break;

            case Opcodes.DCONST_0 :
                stack.push(new JvmObjectConstant(0.0));
                break;

            case Opcodes.DCONST_1 :
                stack.push(new JvmObjectConstant(1.0));
                break;

            case Opcodes.IALOAD :
            case Opcodes.LALOAD :
            case Opcodes.FALOAD :
            case Opcodes.DALOAD :
            case Opcodes.AALOAD :
            case Opcodes.BALOAD :
            case Opcodes.CALOAD :
            case Opcodes.SALOAD :
                {
                    JvmObject index = stack.pop();
                    JvmObject object = stack.pop();

                    stack.push(new JvmObjectArrayLoad(object, index));
                    break;
                }

            case Opcodes.IASTORE :
            case Opcodes.LASTORE :
            case Opcodes.FASTORE :
            case Opcodes.DASTORE :
            case Opcodes.AASTORE :
            case Opcodes.BASTORE :
            case Opcodes.CASTORE :
            case Opcodes.SASTORE :
                {
                    JvmObject value = stack.pop();
                    JvmObject index = stack.pop();
                    JvmObject array = stack.pop();

                    if (array instanceof JvmObjectArray) {
                        ((JvmObjectArray) array).store(index, value);
                    }
                    break;
                }

            case Opcodes.POP :
                stack.pop();
                break;

            case Opcodes.POP2 :
                {
                    JvmObject first = stack.pop();

                    if (!first.isWide()) {
                        stack.pop();
                    }

                    break;
                }

            case Opcodes.DUP :
                stack.push(stack.peek());
                break;

            case Opcodes.DUP_X1 :
                {
                    JvmObject value1 = stack.pop();
                    JvmObject value2 = stack.pop();

                    stack.push(value1);
                    stack.push(value2);
                    stack.push(value1);
                    break;
                }

            case Opcodes.DUP_X2 :
                {
                    JvmObject value1 = stack.pop();
                    JvmObject value2 = stack.pop();

                    if (value2.isWide()) {
                        stack.push(value1);
                        stack.push(value2);
                        stack.push(value1);

                    } else {
                        JvmObject value3 = stack.pop();

                        stack.push(value1);
                        stack.push(value3);
                        stack.push(value2);
                        stack.push(value1);
                    }

                    break;
                }

            case Opcodes.DUP2 :
                {
                    JvmObject value1 = stack.pop();

                    if (value1.isWide()) {
                        stack.push(value1);
                        stack.push(value1);

                    } else {
                        JvmObject value2 = stack.pop();

                        stack.push(value2);
                        stack.push(value1);
                        stack.push(value2);
                        stack.push(value1);
                    }

                    break;
                }

            case Opcodes.DUP2_X1 :
                {
                    JvmObject value1 = stack.pop();
                    JvmObject value2 = stack.pop();
                    JvmObject value3 = stack.pop();

                    stack.push(value2);
                    stack.push(value1);
                    stack.push(value3);
                    stack.push(value2);
                    stack.push(value1);
                    break;
                }

            case Opcodes.DUP2_X2 :
                {
                    JvmObject value1 = stack.pop();
                    JvmObject value2 = stack.pop();
                    JvmObject value3 = stack.pop();
                    JvmObject value4 = stack.pop();

                    stack.push(value2);
                    stack.push(value1);
                    stack.push(value4);
                    stack.push(value3);
                    stack.push(value2);
                    stack.push(value1);
                    break;
                }

            case Opcodes.SWAP :
                {
                    JvmObject value1 = stack.pop();
                    JvmObject value2 = stack.pop();

                    stack.push(value1);
                    stack.push(value2);
                    break;
                }

            case Opcodes.IADD :
            case Opcodes.LADD :
            case Opcodes.FADD :
            case Opcodes.DADD :
            case Opcodes.ISUB :
            case Opcodes.LSUB :
            case Opcodes.FSUB :
            case Opcodes.DSUB :
            case Opcodes.IMUL :
            case Opcodes.LMUL :
            case Opcodes.FMUL :
            case Opcodes.DMUL :
            case Opcodes.IDIV :
            case Opcodes.LDIV :
            case Opcodes.FDIV :
            case Opcodes.DDIV :
            case Opcodes.IREM :
            case Opcodes.LREM :
            case Opcodes.FREM :
            case Opcodes.DREM :
            case Opcodes.ISHL :
            case Opcodes.LSHL :
            case Opcodes.ISHR :
            case Opcodes.LSHR :
            case Opcodes.IUSHR :
            case Opcodes.LUSHR :
            case Opcodes.IAND :
            case Opcodes.LAND :
            case Opcodes.IOR :
            case Opcodes.LOR :
            case Opcodes.IXOR :
            case Opcodes.LXOR :
                {
                    JvmObject right = stack.pop();
                    JvmObject left = stack.pop();

                    stack.push(new JvmObjectBinary(left, opcode, right));
                    break;
                }

            case Opcodes.INEG :
            case Opcodes.LNEG :
            case Opcodes.FNEG :
            case Opcodes.DNEG :
                {
                    JvmObject object = stack.pop();

                    stack.push(new JvmObjectUnary(opcode, object));
                    break;
                }

            case Opcodes.I2L :
            case Opcodes.I2F :
            case Opcodes.I2D :
            case Opcodes.L2I :
            case Opcodes.L2F :
            case Opcodes.L2D :
            case Opcodes.F2I :
            case Opcodes.F2L :
            case Opcodes.F2D :
            case Opcodes.D2I :
            case Opcodes.D2L :
            case Opcodes.D2F :
            case Opcodes.I2B :
            case Opcodes.I2C :
            case Opcodes.I2S :
                break;

            case Opcodes.LCMP :
            case Opcodes.FCMPL :
            case Opcodes.FCMPG :
            case Opcodes.DCMPL :
            case Opcodes.DCMPG :
                {
                    JvmObject right = stack.pop();
                    JvmObject left = stack.pop();

                    stack.push(new JvmObjectBinary(left, opcode, right));
                    break;
                }

            case Opcodes.IRETURN :
            case Opcodes.LRETURN :
            case Opcodes.FRETURN :
            case Opcodes.DRETURN :
            case Opcodes.ARETURN :
                // XXX: stack.pop();
                // XXX: stack.clear();
                break;

            case Opcodes.RETURN :
                // XXX: stack.clear();
                break;

            case Opcodes.ARRAYLENGTH :
                {
                    JvmObject object = stack.pop();

                    stack.push(new JvmObjectArrayLength(object));
                    break;
                }

            case Opcodes.ATHROW :
                // XXX: stack.pop();
                // XXX: stack.clear();
                break;

            case Opcodes.MONITORENTER :
            case Opcodes.MONITOREXIT :
                stack.pop();
                break;

            default :
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void visitIntInsn(int opcode, int operand) {
        switch (opcode) {
            case Opcodes.BIPUSH :
                stack.push(new JvmObjectConstant((byte) operand));
                break;

            case Opcodes.SIPUSH :
                stack.push(new JvmObjectConstant((short) operand));
                break;

            case Opcodes.NEWARRAY :
                {
                    stack.pop();
                    stack.push(new JvmObjectArray(
                            operand == Opcodes.T_BOOLEAN ? Type.BOOLEAN_TYPE
                            : operand == Opcodes.T_CHAR ? Type.CHAR_TYPE
                            : operand == Opcodes.T_FLOAT ? Type.FLOAT_TYPE
                            : operand == Opcodes.T_DOUBLE ? Type.DOUBLE_TYPE
                            : operand == Opcodes.T_BYTE ? Type.BYTE_TYPE
                            : operand == Opcodes.T_SHORT ? Type.SHORT_TYPE
                            : operand == Opcodes.T_INT ? Type.INT_TYPE
                            : operand == Opcodes.T_LONG ? Type.LONG_TYPE
                            : null));
                    break;
                }

            default :
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void visitJumpInsn(int opcode, Label label) {
        int newInstructionIndex = parent.instructions.indexOf(parent.getLabelNode(label));

        switch (opcode) {
            case Opcodes.GOTO :
                if (newInstructionIndex > instructionIndex) {
                    new JvmRunner(this, newInstructionIndex).run();
                }

                instructionIndex = Integer.MIN_VALUE;

                break;

            case Opcodes.IFEQ :
            case Opcodes.IFNE :
            case Opcodes.IFLT :
            case Opcodes.IFGE :
            case Opcodes.IFGT :
            case Opcodes.IFLE :
            case Opcodes.IFNULL :
            case Opcodes.IFNONNULL :
                stack.pop();
                break;

            case Opcodes.IF_ICMPEQ :
            case Opcodes.IF_ICMPNE :
            case Opcodes.IF_ICMPLT :
            case Opcodes.IF_ICMPGE :
            case Opcodes.IF_ICMPGT :
            case Opcodes.IF_ICMPLE :
            case Opcodes.IF_ACMPEQ :
            case Opcodes.IF_ACMPNE :
                stack.pop();
                stack.pop();
                break;

            case Opcodes.JSR :
                // XXX: stack.push?
                break;

            default :
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void visitLabel(Label label) {
    }

    @Override
    public void visitLdcInsn(Object cst) {
        stack.push(new JvmObjectConstant(cst));
    }

    @Override
    public void visitLineNumber(int line, Label start) {
        if (callingMethodLine == 0) {
            callingMethodLine = line;
        }

        lastLine = line;
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

    @Override
    public void visitMethodInsn(int opcode, String owner, String name, String desc) {
        Type[] calledArgumentTypes = Type.getArgumentTypes(desc);
        List<Class<?>> calledArgumentClasses = new ArrayList<Class<?>>();
        List<JvmObject> calledArguments = new ArrayList<JvmObject>();

        for (Type t : calledArgumentTypes) {
            calledArgumentClasses.add(typeToClass(t));
            calledArguments.add(0, stack.pop());
        }

        JvmObject object = null;

        switch (opcode) {
            case Opcodes.INVOKEVIRTUAL :
            case Opcodes.INVOKESPECIAL :
            case Opcodes.INVOKEINTERFACE :
            case Opcodes.INVOKEDYNAMIC :
                object = stack.pop();
                break;

            case Opcodes.INVOKESTATIC :
                object = new JvmObjectStatic(Type.getObjectType(owner));
                break;

            default :
                throw new IllegalArgumentException();
        }

        Class<?> calledMethodClass = typeToClass(Type.getObjectType(owner));
        Class<?>[] calledArgumentClassesArray = calledArgumentClasses.toArray(new Class<?>[calledArgumentClasses.size()]);
        AccessibleObject calledConstructorOrMethod = null;

        if ("<init>".equals(name)) {
            for (Class<?> s = calledMethodClass; s != null; s = s.getSuperclass()) {
                try {
                    calledConstructorOrMethod = s.getDeclaredConstructor(calledArgumentClassesArray);
                    break;

                } catch (NoClassDefFoundError error) {
                    break;

                } catch (NoSuchMethodException error) {
                    // Try the next super class.
                }
            }

            if (calledConstructorOrMethod == null) {
                for (Class<?> i : calledMethodClass.getInterfaces()) {
                    try {
                        calledConstructorOrMethod = i.getDeclaredConstructor(calledArgumentClassesArray);
                        break;

                    } catch (NoClassDefFoundError error) {
                        break;

                    } catch (NoSuchMethodException error) {
                        // Try the next interface.
                    }
                }
            }

        } else {
            for (Class<?> s = calledMethodClass; s != null; s = s.getSuperclass()) {
                try {
                    calledConstructorOrMethod = s.getDeclaredMethod(name, calledArgumentClassesArray);
                    break;

                } catch (NoClassDefFoundError error) {
                    break;

                } catch (NoSuchMethodException error) {
                    // Try the next super class.
                }
            }

            if (calledConstructorOrMethod == null) {
                for (Class<?> i : calledMethodClass.getInterfaces()) {
                    try {
                        calledConstructorOrMethod = i.getDeclaredMethod(name, calledArgumentClassesArray);
                        break;

                    } catch (NoClassDefFoundError error) {
                        break;

                    } catch (NoSuchMethodException error) {
                        // Try the next interface.
                    }
                }
            }
        }

        Type returnType = Type.getReturnType(desc);
        JvmInvocation invocation = object.addInvocation(calledConstructorOrMethod, calledArguments);
        JvmObject returnedObject = null;

        if (!Type.VOID_TYPE.equals(returnType)) {
            returnedObject = new JvmObjectReturn(returnType, object, invocation);

            stack.push(returnedObject);
        }

        for (Map.Entry<Method, List<JvmMethodListener>> entry : parent.getJvm().listenersByMethod.entrySet()) {
            if (entry.getKey().equals(calledConstructorOrMethod)) {
                for (JvmMethodListener listener : entry.getValue()) {
                    listener.onInvocation(
                            parent.getCallingMethod(),
                            callingMethodLine,
                            (Method) calledConstructorOrMethod,
                            lastLine,
                            object,
                            calledArguments,
                            returnedObject);
                }
            }
        }
    }

    @Override
    public void visitMultiANewArrayInsn(String desc, int dims) {
        for (int i = 0; i < dims; ++ i) {
            stack.pop();
        }

        stack.push(new JvmObjectArray(Type.getType(desc)));
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
        switch (opcode) {
            case Opcodes.NEW :
                stack.push(new JvmObjectNew(Type.getObjectType(type)));
                break;

            case Opcodes.ANEWARRAY :
                stack.pop();
                stack.push(new JvmObjectArray(Type.getObjectType(type)));
                break;

            case Opcodes.INSTANCEOF :
                stack.pop();
                stack.push(new JvmObjectConstant(Boolean.TRUE));
                break;

            case Opcodes.CHECKCAST :
                break;

            default :
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void visitVarInsn(int opcode, int var) {
        switch (opcode) {
            case Opcodes.ILOAD :
            case Opcodes.LLOAD :
            case Opcodes.FLOAD :
            case Opcodes.DLOAD :
            case Opcodes.ALOAD :
                {
                    JvmObject local = locals.get(var);

                    stack.push(local == null
                            ? new JvmObjectLocal(parent.getLocalType(var), parent.getLocalName(var))
                            : local);
                    break;
                }

            case Opcodes.ISTORE :
            case Opcodes.LSTORE :
            case Opcodes.FSTORE :
            case Opcodes.DSTORE :
            case Opcodes.ASTORE :
                {
                    JvmObject value = stack.pop();

                    locals.put(var, value);
                    break;
                }

            case Opcodes.RET :
                break;

            default :
                throw new IllegalArgumentException();
        }
    }
}
