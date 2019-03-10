package org.easytool.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringUtils {


    public static boolean isNull(Object o){
        return o==null;
    }
    public static boolean isEmpty(String str){
        return str==null || str.length()==0;
    }
    public static boolean isNotEmpty(String str){
        return str!=null && str.length()>0;
    }
    public static boolean isBlank(String str){
        return str!=null && str.length()==0;
    }

    public static String getStringDef(String obj,String def){
        return isNull(obj)?def:obj;
    }
    public static String getFirstNotEmpty(String ... str){
        for (int i = 0; i < str.length; i++) {
            if(isNotEmpty(str[i])){
                return str[i];
            }
        }
        return null;
    }
    public static int getStrToIntRange(String str,int start,int end,int def){
        try {
            int i = Integer.valueOf(str);
            if (i>=start && i <=end){
                return i;
            }
        } catch (Exception e) {
        }
        return def;
    }

    public static Map<String,Object> argsToMap2(String[] args){
        Map<String,Object> result = new HashMap<String, Object>();
        List<String> firstList = new ArrayList<String>();
        List<String> lastList = new ArrayList<String>();
        boolean isFirst = true; boolean isLast = false;
        // xx action... -option... params...
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if(!isLast && flag.startsWith("-") ){
                if(isFirst){
                    result.put("args-action-list",firstList);
                }
                isFirst = false;
                if(args.length>=i){
                    result.put(flag,args[i+1]);
                    i++;
                }else{
                    result.put(flag,"");
                }
            }else{
                if(isFirst){
                    firstList.add(flag);
                }else{
                    isLast = true;
                    lastList.add(flag);
                }
            }
        }
        if(isLast){
            result.put("args-params-list",lastList);
        }
        return result;
    }

    public static String getActionValue(Map<String,Object> args,int index){
        Object list = args.get("args-action-list");
        if(list instanceof List){
            List ls = (List)list;
            if(index>=0 && index <= ls.size()){
                return (String)ls.get(index);
            }
        }
        return "";
    }
    public static String getParamsValue(Map<String,Object> args, int index){
        Object list = args.get("args-params-list");
        if(list instanceof List){
            List ls = (List)list;
            if(index>=0 && index <= ls.size()){
                return (String)ls.get(index);
            }
        }
        return "";
    }


    public static String getArgs(String[] args,String flag){
        for (int i = 0; i < args.length; i++) {
            if(args[i].equals(flag)){
                if(args.length>=i){
                    return args[i+1];
                }
            }
        }
        return "";
    }
    public static Map<String,String> argsToMap(String[] args){
        Map<String,String> result = new HashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if(flag.startsWith("-")){
                if(args.length>=i){
                    result.put(flag,args[i+1]);
                    i++;
                }else{
                    result.put(String.valueOf(i),flag);
                }
            }else{
                result.put(String.valueOf(i),flag);
            }
        }
        return result;
    }
}
