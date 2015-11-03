package com.psddev.dari.util;

import java.io.IOException;

/**
 * @deprecated Use {@link StorageItemAfterSave} instead
 */

@Deprecated
public interface StorageItemListener {

    public void afterSave(StorageItem item) throws IOException;

}
