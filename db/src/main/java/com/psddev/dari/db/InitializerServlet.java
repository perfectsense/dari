package com.psddev.dari.db;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.DependencyResolver;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.TypeDefinition;

/** @deprecated No replacement. */
@DebugFilter.Path("init")
@Deprecated
@SuppressWarnings("serial")
public class InitializerServlet extends HttpServlet {

    private static final Map<Class<?>, Initializer> INITIALIZERS = new com.psddev.dari.util.PullThroughCache<Class<?>, Initializer>() {

        @Override
        protected Initializer produce(Class<?> initClass) {
            return (Initializer) TypeDefinition.getInstance(initClass).newInstance();
        }
    };

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        PrintWriter writer = response.getWriter();
        DependencyResolver<Initializer> initializersResolver = new DependencyResolver<Initializer>();

        for (Class<?> initClass : ObjectUtils.findClasses(Initializer.class)) {
            int mod = initClass.getModifiers();
            if (!(Modifier.isAbstract(mod) || Modifier.isInterface(mod))) {
                Initializer initializer = INITIALIZERS.get(initClass);
                Set<Class<? extends Initializer>> dependencies = initializer.dependencies();
                if (dependencies == null) {
                    initializersResolver.addRequired(initializer);
                } else {
                    for (Class<? extends Initializer> dependencyClass : dependencies) {
                        initializersResolver.addRequired(initializer, INITIALIZERS.get(dependencyClass));
                    }
                }
            }
        }

        Database database = Database.Static.getDefault();
        com.psddev.dari.util.StringLogger logger = new com.psddev.dari.util.StringLogger();

        for (Initializer initializer : initializersResolver.resolve()) {
            logger.reset();

            try {
                initializer.execute(database, logger);

            } catch (Exception ex) {
                writer.println("<p><pre>");
                ex.printStackTrace(writer);
                writer.println("</pre></p>");
            }

            writer.print("<h2>Executing ");
            writer.print(initializer.getClass().getName());
            writer.print("</h2><p><pre>");
            writer.print(logger.toString());
            writer.println("</pre></p>");
        }
    }
}
