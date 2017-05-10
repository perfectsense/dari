package com.psddev.dari.h2;

import com.psddev.dari.db.Query;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.*;

public class StateTest extends AbstractTest {

    @After
    public void deleteModels() {

        Query.from(UuidIndexModel.class).deleteAll();
    }

    @Test
    public void concurrentResolveReferences() throws Exception {

        int numThreads = 20;

        UuidIndexModel model1 = new UuidIndexModel();
        UuidIndexModel model2 = new UuidIndexModel();
        model1.setReferenceOne(model2);

        model1.save();
        model2.save();

        List<Thread> threads = new ArrayList<>();
        final CyclicBarrier gate = new CyclicBarrier(numThreads);
        final CyclicBarrier end = new CyclicBarrier(numThreads + 1);
        final List<Object> references = Collections.synchronizedList(new ArrayList<>());
        final List<String> errors = Collections.synchronizedList(new ArrayList<>());

        final UuidIndexModel dbModel1 = Query.from(UuidIndexModel.class).noCache().master().where("_id = ?", model1.getId()).first();

        for (int i = 0; i < numThreads; i += 1) {
            threads.add(new Thread(() -> {

                try {
                    gate.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    errors.add(e.getClass().getName() + ": " + e.getMessage());
                    return;
                }

                UuidIndexModel dbModel1ReferenceOne = dbModel1.getReferenceOne();

                references.add(dbModel1ReferenceOne);

                try {
                    end.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    errors.add(e.getClass().getName() + ": " + e.getMessage());
                    return;
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        end.await();

        if (errors.size() > 0) {
            errors.forEach(System.out::println);
            throw new Exception("Encountered " + errors.size() + " concurrency errors during test!");
        }

        assertTrue(!references.stream().anyMatch(o -> o == null));
    }
}
