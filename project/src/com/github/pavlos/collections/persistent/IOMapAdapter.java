package com.github.pavlos.collections.persistent;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * This class adapts IOMap to the Map interface by translating the checked
 * exceptions to RuntimeExceptions
 * @author Paul Hieromnimon
 */
public class IOMapAdapter<K extends Serializable, V extends Serializable>
        implements Map<K,V>{

    private IOMap<K,V> map;

    public IOMapAdapter(IOMap<K,V> map){
        this.map = map;
    }

    public int size() {
        return map.size();
    }

    public V remove(Object key) {
        try{
            return map.remove(key);
        } catch (IOException e){
            throw new RuntimeIOException(e);
        }
    }

    public V put(K key, V value) {
        try{
            return map.put(key, value);
        } catch (IOException e){
            throw new RuntimeIOException(e);
        }
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public V get(Object key) {
        try{
            return map.get(key);
        } catch (IOException e){
            throw new RuntimeIOException(e);
        }
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public void clear() {
        map.clear();
    }

    public boolean containsValue(Object value) {
        try{
            return map.containsValue(value);
        } catch (IOException e){
            throw new RuntimeIOException(e);
        }
    }

    public Set<Entry<K, V>> entrySet() {
        try{
            return map.entrySet();
        } catch (IOException e){
            throw new RuntimeIOException(e);
        }
    }

    public Set<K> keySet() {
         try{
            return map.keySet();
        } catch (IOException e){
             throw new RuntimeIOException(e);
        }
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        try{
            map.putAll(m);
        } catch (IOException e){
             throw new RuntimeIOException(e);           
        }
    }

    public Collection<V> values() {
        try{
            return map.values();
        } catch (IOException e){
             throw new RuntimeIOException(e);
        }
    }
}