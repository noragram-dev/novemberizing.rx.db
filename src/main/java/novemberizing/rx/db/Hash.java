package novemberizing.rx.db;

import com.google.gson.Gson;
import novemberizing.rx.Observable;

import redis.clients.jedis.Jedis;


/**
 *
 * @author novemberizing, me@novemberizing.net
 * @since 2017. 2. 9.
 */
@SuppressWarnings({"DanglingJavadoc", "WeakerAccess", "unused", "Convert2Lambda"})
public class Hash extends Observable<novemberizing.ds.tuple.Triple<Integer,String, String>> implements Runnable {

    public static <T> novemberizing.rx.Req<T> Set(String category, String key, T o, Gson gson){
        novemberizing.rx.Req<T> req = novemberizing.rx.Operator.Req(category, key, o,
                (__category, __key, value, res)->{
                    Jedis jedis = redis.Pool.Jedis();
                    try {
                        jedis.hset(__category, __key, gson.toJson(value));
                    } catch(Exception e){
                        res.error(e);
                    } finally {
                        res.complete();
                    }
                });
        req.execute(null);
        return req;
    }

    public static novemberizing.rx.Req<Object> Del(String category, String key){
        novemberizing.rx.Req<Object> req = novemberizing.rx.Operator.Req(category, key,
                (__category, __key, res)->{
                    Jedis jedis = redis.Pool.Jedis();
                    try {
                        jedis.hdel(__category, __key);
                        res.complete();
                    } catch(Exception e){
                        res.error(e);
                    }
                });
        req.execute(null);
        return req;
    }


    public static class Req {
        public static <T> novemberizing.rx.Req.Factory<T> Set(String category, String key, T o, Gson gson){
            return new novemberizing.rx.Req.Factory<T>(){
                @Override public novemberizing.rx.Req<T> call() { return Hash.Set(category, key, o, gson); }
            };
        }
        public static novemberizing.rx.Req.Factory<Object> Del(String category, String key){
            return new novemberizing.rx.Req.Factory<Object>(){
                @Override public novemberizing.rx.Req<Object> call() { return Hash.Del(category, key); }
            };
        }
    }

    @Override public void run() {}

//    public static novemberizing.rx.Req.Factory<String> Req(String category, String parent, String child, String o){
//        return new novemberizing.rx.Req.Factory<String>(){
//            @Override
//            public novemberizing.rx.Req<String> call() {
//                return novemberizing.rx.db.Hash.Set(category, parent, child, o);
//            }
//        };
//    }
//
//    public static String Get(String category, String parent, String child){
//        Jedis jedis = redis.Pool.Jedis();
//        String ret = null;
//        try {
//            ret = jedis.hget(category + ":" + parent, child);
//        } catch(Exception e){
//            Log.e("redis>", e.getMessage());
//        }
//        return ret;
//    }
//
//    public static novemberizing.rx.Req<String> Set(String category, String parent, String child, String o){

//    }
//


//    private boolean __cancel;
//    private JedisPubSub __subscriber;
//    private String __category;
//    private String __parent;
//    private Map<String, String> __map;
//
//    public String key(){ return __category + ":" + __parent; }
//    public String category(){ return __category; }
//    public String parent(){ return __parent; }
//
//    public Hash(String category, String key){
//        __category = category;
//        __parent = key;
//    }
//
//    public Hash on(){
//        if(!__cancel){ new Thread(this).start(); }
//        return this;
//    }
//
//    public Hash off(){
//        if(__subscriber!=null){ __subscriber.unsubscribe(); }
//        __cancel = true;
//        return this;
//    }
//
//    private void onHashSet(String[] strings){
//
//        Jedis jedis = redis.Pool.Jedis();
//        try {
//            String v = jedis.hget(key(), strings[1]);
//            emit(new novemberizing.ds.tuple.Triple<>(Data.SET,strings[1], v));
//        } catch(Exception e){
//            error(e);
//        }
//    }
//
//    private void onHashDel(String[] strings){
//        emit(new novemberizing.ds.tuple.Triple<>(Data.DEL,strings[1], __map.get(strings[1])));
//        __map.remove(strings[1]);
//    }
//
//    private void onHashMultiSet(String[] strings){
//        Jedis jedis = redis.Pool.Jedis();
//        try {
//            String[] keys = new String[strings.length-1];
//            List<String> values = jedis.hmget(key(), keys);
//            for(int i=0;i<keys.length;i++){
//                String v = values.get(i);
//                __map.put(keys[i], v);
//                emit(new novemberizing.ds.tuple.Triple<>(Data.SET,strings[1], v));
//            }
//        } catch(Exception e){
//            error(e);
//        }
//    }
//
//    @Override
//    public void run() {
//        __cancel = false;
//        while(!__cancel){
//            Jedis jedis = redis.Pool.Jedis();
//            __map = jedis.hgetAll(key());
//            __subscriber = new JedisPubSub() {
//                @Override
//                public void onMessage(String channel, String message) {
//                    try {
//                        String[] strings = message.split("[ ]");
//                        if(strings.length>1){
//                            switch(strings[0]){
//                                case "hset": onHashSet(strings); break;
//                                case "hmset": onHashDel(strings); break;
//                                case "hdel": onHashMultiSet(strings); break;
//                                case "hincrby": onHashSet(strings); break;
//                                case "hincrbyfloat": onHashSet(strings); break;
//                                default:    break;
//                            }
//                        } else {
//                            error(new Throwable("not support install new redis."));
//                        }
//                    } catch(Exception e){
//                        error(e);
//                    }
//                    super.onMessage(channel, message);
//                }
//            };
//            jedis.subscribe(__subscriber, "__realtime@0__:" + key());
//        }
//    }
}
