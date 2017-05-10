package com.psddev.dari.db;

import com.psddev.dari.util.Task;
import com.psddev.dari.util.TaskListener;

/**
 * {@link TaskListener} implementation that calls
 * {@link Database#resetThreadLocals()} before each run.
 */
public class ResetTaskListener implements TaskListener {

    @Override
    public void before(Task task) {
        Database.resetThreadLocals();
    }
}
