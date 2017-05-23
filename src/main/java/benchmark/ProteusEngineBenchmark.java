package benchmark;


import common.CommonConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.util.SideInput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.RollingSink;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by jeka01 on 12.04.17.
 */
public class ProteusEngineBenchmark {
    public static void main (String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(4);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        if (args.length != 1){
            System.err.println("Configuration file is missing");
            System.exit(0);
        }
        CommonConfig.initializeConfig(args[0]);
        DataStream<Tuple2<String,String>> sideSource = env.readTextFile(CommonConfig.SIDE_INPUT_PATH()).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                String coilID = fields[2];
                String features = "";
                for (int i = 3; i < fields.length; i++){
                    features = features + fields[i] + ",";

                }
                return new Tuple2<String, String>(coilID, features);
            }
        });
        final SideInput<Tuple2<String, String>> sideInput = env.newKeyedSideInput(sideSource, 0);


        DataStream<String> socketSource = null;
        for (String host : CommonConfig.DATASOURCE_HOSTS()) {
            for (int port: CommonConfig.DATASOURCE_PORTS()){
                DataStream<String> socketSource_i = env.socketTextStream(host, port);
                socketSource = socketSource == null ? socketSource_i : socketSource.union(socketSource_i);
            }
        }
        DataStream<Tuple3<Long, String, String>> realTimeDataStream = socketSource.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                Long ts ;
                try {
                    ts = new Long( fields[0]);
                } catch (Exception e) {
                    ts = -1L;
                }
                String coilID = fields[3];
                String features = "";
                for (int i = 4; i < fields.length; i++){
                    features = features + fields[i] + ",";
                }
                return new Tuple3<Long, String, String>(ts, coilID, features);
            }
        });

        DataStream<Tuple4<Long, Long, String, Integer>> resultStream = realTimeDataStream.keyBy(1).
                map(new RichMapFunction<Tuple3<Long, String, String>, Tuple4<Long, Long, String, Integer>>() {
                    private HashMap<String, Integer> joinHM = null;

                    @Override
                    public Tuple4<Long, Long, String, Integer> map(Tuple3<Long, String, String> tuple) throws Exception {
                        if (joinHM == null) {
                            joinHM = new HashMap<>();
                            ArrayList<Tuple2<String, String>> sideRecords = (ArrayList<Tuple2<String, String>>) getRuntimeContext().getSideInput(sideInput);
                            for (Tuple2<String, String> sideTuple: sideRecords) {
                                joinHM.put(sideTuple.f0 + sideTuple.f1, joinHM.getOrDefault(sideTuple.f0 + sideTuple.f1, 0) +1);
                            }
                        }

                        return new Tuple4<>(System.currentTimeMillis() - tuple.f0 , tuple.f0, tuple.f1, joinHM.getOrDefault(tuple.f1 + tuple.f2, 0));
                    }
                }).withSideInput(sideInput);
        DataStream<Tuple4<Long, Long, String, Integer>> filteredStream = resultStream.filter(new FilterFunction<Tuple4<Long, Long, String, Integer>>() {
            @Override
            public boolean filter(Tuple4<Long, Long, String, Integer> t) throws Exception {
                return t.f3 > CommonConfig.SIMILARITY_THRESHOLD() ;
            }
        });
        RollingSink sink = new RollingSink<String>(CommonConfig.FLINK_OUTPUT());
        sink.setBatchSize(1024 * CommonConfig.OUTPUT_BATCHSIZE_KB());

        filteredStream.addSink(sink);

        env.execute();
    }
}
