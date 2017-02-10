package redis.rx;

import com.google.gson.Gson;
import novemberizing.rx.Observable;
import novemberizing.rx.Subscribers;

/**
 *
 * @author novemberizing, me@novemberizing.net
 * @since 2017. 2. 10.
 */
public class Data<T> extends Observable<T> {
    public final static int SET = 1;
    public final static int DEL = 2;

    private static final String Tag = "redis.rx>";

    private final redis.rx.Hash __hash;
    private final Class<T> __c;
    private final String __child;
    private final Gson __gson;
    private final Subscribers.Just<novemberizing.ds.tuple.Triple<Integer, String, String>> __subscriber =
            new Subscribers.Just<novemberizing.ds.tuple.Triple<Integer, String, String>>(Tag) {
                @Override
                public void onNext(novemberizing.ds.tuple.Triple<Integer, String, String> item){
                    if(item!=null && __child.equals(item.second)){
                        if(item.first==SET) {
                            emit(__gson.fromJson(item.third, __c));
                        } else {
                            emit(null);
                        }
                    }
                }
            };

    public Data(redis.rx.Hash hash, String child, Class<T> c, Gson gson){
        __hash = hash;
        __c = c;
        __child = child;
        __gson = gson;
    }

    public String category(){ return __hash.category(); }
    public String parent(){ return __hash.parent(); }
    public String key(){ return __hash.key(); }

    public Data<T> on(){
        __hash.on();
        return this;
    }

    public Data<T> off(){
        __hash.off();
        return this;
    }

    @Override public T get(){ return super.get(); }


}
