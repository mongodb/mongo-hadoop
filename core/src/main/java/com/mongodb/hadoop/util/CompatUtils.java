package com.mongodb.hadoop.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public final class CompatUtils {

    private CompatUtils() {}

    /**
     * Proxy interface for {@link org.apache.hadoop.mapreduce.TaskAttemptContext}
     * that allows us to support both Hadoop 1.X and 2.X with the same library.
     */
    public interface TaskAttemptContext extends Progressable {
        Configuration getConfiguration();
        TaskAttemptID getTaskAttemptID();
    }

    private interface CompatProxy {}

    private static class CompatibleInvocationHandler
      implements InvocationHandler {

        private final Object target;

        public CompatibleInvocationHandler(final Object target) {
            this.target = target;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args)
          throws Throwable {
            Method m = target.getClass().getMethod(
              method.getName(),
              method.getParameterTypes());
            return m.invoke(target, args);
        }
    }

    public static TaskAttemptContext getTaskAttemptContext(
      final org.apache.hadoop.mapreduce.TaskAttemptContext context) {
        return (TaskAttemptContext) Proxy.newProxyInstance(
          TaskAttemptContext.class.getClassLoader(),
          new Class[]{CompatProxy.class, TaskAttemptContext.class},
          new CompatibleInvocationHandler(context));
    }

    public static TaskAttemptContext getTaskAttemptContext(
      final Configuration conf, final String taskID) {
        return new TaskAttemptContext() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                return TaskAttemptID.forName(taskID);
            }

            @Override
            public void progress() {
                // Do nothing.
            }
        };
    }

    public static <U> Class<? extends U> loadClass(
      final Configuration conf,
      final String className,
      final Class<U> xface) {

        try {
            Class<? extends U> klass = MongoConfigUtil.getClassByName(
              conf, className, xface);
            // Cache class in the configuration.
            conf.setClass(className, klass, xface);
            return klass;
        } catch (Exception e) {
            return null;
        }
    }

    public static boolean isInstance(
      final Object test,
      final String className,
      final Configuration conf,
      final Class<?> xface) {
        Class<?> testAgainst = loadClass(conf, className, xface);
        return testAgainst != null && test.getClass().isInstance(testAgainst);
    }

    public static Object invokeMethod(
      final Class<?> xface,
      final Object invokee,
      final String methodName,
      final Object[] parameters,
      final Class[] parameterTypes) {

        Object asSubclass = xface.cast(invokee);
        try {
            Method method =
              asSubclass.getClass().getMethod(methodName, parameterTypes);
            return method.invoke(invokee, parameters);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException(e);
        } catch (InvocationTargetException e) {
            throw new UnsupportedOperationException(e);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
