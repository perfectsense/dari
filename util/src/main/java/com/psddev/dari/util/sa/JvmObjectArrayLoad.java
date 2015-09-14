package com.psddev.dari.util.sa;

public class JvmObjectArrayLoad extends JvmObject {

    private final JvmObject object;
    private final JvmObject index;

    public JvmObjectArrayLoad(JvmObject object, JvmObject index) {
        super(object.type);

        this.object = object;
        this.index = index;
    }

    @Override
    protected Object doResolve() {
        Object object = this.object.resolve();
        Object index = this.index.resolve();

        if (object instanceof Object[]
                && index instanceof Integer) {
            return ((Object[]) object)[((Integer) index).intValue()];

        } else {
            return null;
        }
    }

    @Override
    public JvmObjectArrayLoad clone() {
        return updateClone(new JvmObjectArrayLoad(object.clone(), index.clone()));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append(object);
        sb.append('[');
        sb.append(index);
        sb.append(']');
    }
}
