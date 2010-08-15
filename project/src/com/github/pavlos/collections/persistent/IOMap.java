package com.github.pavlos.collections.persistent;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Collection;
import java.util.Set;
import java.util.Map.Entry;

/**
 * A map that performs IO operations on Entries, thus is prone to throwing
 * IOException, thereby breaking the contract of java.util.Map
 *
 * The methods in
 *
 * @author Paul Hieromnimon
 */
public interface IOMap<K extends Serializable, V extends Serializable> {

    void clear();

    boolean containsKey(Object key);

    /**
     *
     * @param key
     * @return Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * @throws IOException
     */
    V get(Object key) throws IOException;

    boolean isEmpty();

    V put(K key, V value) throws IOException;

    V remove(Object key) throws IOException;

    void putAll(Map<? extends K, ? extends V> m) throws IOException;

    int size();
    
    public boolean containsValue(Object value) throws IOException;

    public Collection<V> values() throws IOException;

    public Set<K> keySet() throws IOException;
    
    public Set<Entry<K, V>> entrySet() throws IOException;
}
