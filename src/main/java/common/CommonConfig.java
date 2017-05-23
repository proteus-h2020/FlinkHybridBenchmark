package common;

import com.esotericsoftware.yamlbeans.YamlReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jeka01 on 03/11/2016.
 */
public class CommonConfig {
    private static final Logger LOG = LoggerFactory.getLogger(CommonConfig.class);


    private static String FLINK_OUTPUT = "flink.output";


    private static String OUTPUT_BATCHSIZE_KB = "output.batchsize.kb";

    private static String SIDE_INPUT_PATH = "input.side.path";
    private static String DATASOURCE_PORTS = "datasourcesocket.ports";
    private static String DATASOURCE_HOSTS = "datasourcesocket.hosts";
    private static String SIMILARITY_THRESHOLD = "similarity.threshold";



    private static HashMap instance = null;

    public static void initializeConfig(String confPath) {
        try {
            YamlReader reader = new YamlReader(new FileReader(confPath));
            Object object = reader.read();
            instance = (HashMap) object;
        } catch (Exception e) {
            LOG.error("Error in creating config object");
        }
    }

    public static List<Integer> DATASOURCE_PORTS() {
        List<Integer> myList = new ArrayList<>();
        for(String port: (List<String> )instance.get(DATASOURCE_PORTS)){
            myList.add(new Integer(port));
        }
        return myList;
    }
    public static List<String> DATASOURCE_HOSTS() {
        return (List<String>) instance.get(DATASOURCE_HOSTS);
    }

    public static String SIDE_INPUT_PATH () {
        return instance.get(SIDE_INPUT_PATH).toString();
    }
    public static String FLINK_OUTPUT () {
        return instance.get(FLINK_OUTPUT).toString();
    }


    public static int OUTPUT_BATCHSIZE_KB () {
        return new Integer(instance.get(OUTPUT_BATCHSIZE_KB).toString());
    }
    public static int SIMILARITY_THRESHOLD () {
        return new Integer(instance.get(SIMILARITY_THRESHOLD).toString());
    }


}
