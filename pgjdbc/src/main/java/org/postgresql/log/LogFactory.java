package org.postgresql.log;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import static org.postgresql.util.PSQLState.INVALID_PARAMETER_VALUE;

public class LogFactory {

    public static Log getLogger(String className, String instanceName) throws SQLException {
        // className null ,throw exception
        if (className == null) {
            throw new PSQLException("logger class name is null", INVALID_PARAMETER_VALUE);
        }
        //instanceName null, throw exception
        if (instanceName == null) {
            throw new PSQLException("logger instance name is null", INVALID_PARAMETER_VALUE);
        }

        try {
            Class<?> loggerClass = null;
            try {
                loggerClass = Class.forName(className);
            } catch (ClassNotFoundException en) {
                loggerClass = Class.forName(getPackageName(Log.class) + "." + className);
            }
            Constructor<?> constructor = loggerClass.getConstructor(new Class[]{String.class});
            return (Log) (constructor.newInstance(new Object[]{instanceName}));
        } catch (ClassNotFoundException cnfe) {
            throw new PSQLException("can't find class of logger '"+className+"'", INVALID_PARAMETER_VALUE, cnfe);
        } catch (NoSuchMethodException nsme) {
            throw new PSQLException("logger has no default constructor", INVALID_PARAMETER_VALUE, nsme);
        } catch (InstantiationException inste){
            throw new PSQLException("can't instantiate logger class '" + className + "'", INVALID_PARAMETER_VALUE, inste);
        } catch (InvocationTargetException invoe){
            throw new PSQLException("can't invoke target of logger class '" + className + "'", INVALID_PARAMETER_VALUE, invoe);
        } catch (IllegalAccessException ille){
            throw new PSQLException("can't access constructor of logger class '"+ className + "'", INVALID_PARAMETER_VALUE, ille);
        } catch (ClassCastException ce){
            throw new PSQLException("can't cast to Log for logger class '" + className + "'", INVALID_PARAMETER_VALUE, ce);
        }
    }

    public static String getPackageName(Class<?> clazz) {
        String name = clazz.getName();
        int startIndex = name.lastIndexOf('.');
        if (startIndex > 0) {
            return name.substring(0, startIndex);
        }
        return "";
    }

}
