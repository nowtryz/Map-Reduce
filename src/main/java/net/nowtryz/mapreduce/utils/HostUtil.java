package net.nowtryz.mapreduce.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostUtil {
    public static String getHostName() {

        // try InetAddress.LocalHost first;
        //      NOTE -- InetAddress.getLocalHost().getHostName() will not work in certain environments.
        try {
            String result = InetAddress.getLocalHost().getHostName();
            if (!"".equals( result))
                return result;
        } catch (UnknownHostException e) {
            // failed;  try alternate means.
        }

        // try environment properties.
        //
        String host = System.getenv("COMPUTERNAME");
        if (host != null)
            return host;
        host = System.getenv("HOSTNAME");
        if (host != null)
            return host;

        // undetermined.
        return null;
    }

    public static Integer getCpuNumber() {

        return Runtime.getRuntime().availableProcessors();
    }

    public static Long getRamNumber(){
        return Runtime.getRuntime().freeMemory();
    }

}
