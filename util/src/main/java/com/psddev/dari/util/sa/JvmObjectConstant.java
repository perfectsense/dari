package com.psddev.dari.util.sa;

import org.objectweb.asm.Type;

public class JvmObjectConstant extends JvmObject {

    private final Object constant;

    public JvmObjectConstant(Object constant) {
        super(
                constant == null ? null
                : constant instanceof Integer ? Type.INT_TYPE
                : constant instanceof Long ? Type.LONG_TYPE
                : constant instanceof Float ? Type.FLOAT_TYPE
                : constant instanceof Double ? Type.DOUBLE_TYPE
                : constant instanceof Byte ? Type.BYTE_TYPE
                : constant instanceof Short ? Type.SHORT_TYPE
                : constant instanceof Character ? Type.CHAR_TYPE
                : constant instanceof String ? Type.getType(String.class)
                : constant instanceof Type ? Type.getType(Class.class)
                : null);

        this.constant = constant;
    }

    public Object getConstant() {
        return constant;
    }

    @Override
    protected Object doResolve() {
        Object constant = getConstant();

        if (constant instanceof Type) {
            return JvmRunner.typeToClass((Type) constant);

        } else {
            return constant;
        }
    }

    @Override
    public JvmObjectConstant clone() {
        return updateClone(new JvmObjectConstant(constant));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        Object constant = getConstant();

        if (constant instanceof Type) {
            sb.append(typeToName((Type) constant));
            sb.append(".class");

        } else if (constant instanceof String) {
            sb.append('"');
            sb.append(constant);
            sb.append('"');

        } else {
            sb.append(constant);
        }
    }
}
