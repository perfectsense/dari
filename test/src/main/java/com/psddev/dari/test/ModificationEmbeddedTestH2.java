package com.psddev.dari.test;

import com.psddev.dari.db.Query;
import com.psddev.dari.util.TypeDefinition;
import org.hamcrest.beans.HasPropertyWithValue;
import org.hamcrest.core.Every;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ModificationEmbeddedTestH2 extends ModificationEmbeddedTest {

    // H2 issue
    @Override
    @Test
    public void testSingleString() {
    }


}

