package com.example.demo.util;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.*;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

/**
 * Created by liuxing on 2017/11/29.
 */
public class ReflectionUtil {

    public static Object getParamValue(Class<?> clas,Object value){
        if(value == null)return null;
        if(clas.equals(value.getClass())){
            return value;
        }else if(Byte.class.equals(clas)){
            return new ByteConverter().convert(clas, value);
        }else if(Short.class.equals(clas)){
            return new ShortConverter().convert(clas, value);
        }else if(Integer.class.equals(clas)){
            return new IntegerConverter().convert(clas, value);
        }else if(Float.class.equals(clas)){
            return new FloatConverter().convert(clas, value);
        }else if(Double.class.equals(clas)){
            return new DoubleConverter().convert(clas, value);
        }else if(Long.class.equals(clas)){
            return new LongConverter().convert(clas, value);
        }else if(Date.class.equals(clas)){
            DateConverter dateConverter = new DateConverter();
            dateConverter.setPatterns(new String[]{"yyyy-MM-dd","yyyy-MM-dd HH:mm:ss"});
            ConvertUtils.register(dateConverter,Date.class);
            return dateConverter.convert(clas, value);
        }else if(Boolean.class.equals(clas)){
            return new BooleanConverter().convert(clas, value);
        }else if(Character.class.equals(clas)){
            return new CharacterConverter().convert(clas, value);
        }
        return null;
    }

    /**
     * 设置字段 值
     * @param o 实例化的对象
     * @param methodName 参数名
     * @param value 要设置的值
     */
    public static boolean setFileValue(Object o,String methodName,final Object value) throws InvocationTargetException, IllegalAccessException {
        if(value == null)return false;
        //使用该方法要指定参数类型
        //Method method = ReflectionUtils.findMethod(ExcelPojo.class, "setOrdersNumber",String.class);
        Method[] methods = ReflectionUtils.getAllDeclaredMethods(o.getClass());
        for (Method method:methods){
            if(method.getName().startsWith("set") && method.getName().equals(methodName)){
                Class<?>[] paramTypes = method.getParameterTypes();
                method.invoke(o, getParamValue(paramTypes[0], value));
                //ReflectionUtils.invokeMethod(method, cls, new Object[]{});
                break;
            }
        }
        return true;
    }

    /**
     * 获取值
     * @param cls 实例
     * @param methodName 获取的字段方法名
     * @return
     */
    public static Object getFileValue(Object cls,String methodName){
        //使用该方法要指定参数类型
        //Method method = ReflectionUtils.findMethod(ExcelPojo.class, "setOrdersNumber",String.class);
        Method[] methods = ReflectionUtils.getAllDeclaredMethods(cls.getClass());
        for (Method method:methods) {
            if(method.getName().equals(methodName)){
                return ReflectionUtils.invokeMethod(method, cls);
            }
        }
        return null;
    }

    /**
     * 实例化对象
     * @param class1
     * @return
     * @throws Exception
     */
    public static Object newInstance(Class<?> class1) throws Exception{
        Constructor<?> constructor = class1.getConstructor();
        return constructor.newInstance();
    }

}
