package com.github.pavlos.collections.persistent;

import java.io.Serializable;
import java.io.*;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;


/**
 *
 * A crude hash map that stores data in the file system.
 * ASSUMES IF HASH CODES ARE EQUAL, KEYS MUST BE EQUAL - this is a flaw that will
 * soon be fixed, using proper "buckets" (subdirectories) for hash codes
 *
 * @author Paul Hieromnimon
 */
public class FileSystemHashMap<K extends Serializable, V extends Serializable> implements IOMap<K,V>{

    private final File dataStore;

    /**
     *
     * @param dataStore location of where you want entries to be stored
     * @throws IOException
     */
    public FileSystemHashMap(String dataStore) throws IOException{
        // TODO: add a constructor that takes an instance of File
        this.dataStore = new File(dataStore + File.separator + "data");
        this.dataStore.mkdirs();
    }

    /**
     *
     * @return number of entries in this map
     */
    public int size() {
        // TODO: speed this up by caching size as an instance member
        return getSize();
    }

    /**
     *
     * @return true if the map has no entries, otherwise false
     */
    public boolean isEmpty() {
        return getSize() == 0;
    }

    /**
     *
     * @param key
     * @return true if the map contains the key, otherwise false
     */
    public boolean containsKey(Object key) {
        File f = generateFile(key);
        return f.exists();
    }

    /**
     *
     * @param key
     * @return Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * @throws IOException
     */
    public V get(Object key) throws IOException{
        Entry<? extends K,? extends V> entry = getEntry(generateFilename(key));
        if (entry == null){
            return null;
        } else {
            return entry.getValue();
        }
    }

    /**
     * adds or replaces a key value pair in this map
     * @param key
     * @param value
     * @return previous value associated with this key, or null if none
     * @throws IOException
     */
    public V put(K key, V value) throws IOException {
        // return the value previously stored
        V previous = get(key);
        String filename = generateFilename(key);

        ObjectOutputStream os = new ObjectOutputStream(
                                    new FileOutputStream(filename)
                                    );
        os.writeObject(key);
        os.writeObject(value);
        os.close();

        return previous;
    }

    /**
     *
     * @param key - key of the entry to delete
     * @return value previously stored in the entry
     * @throws IOException
     */
    public V remove(Object key) throws IOException {
        // need to return the previous value before removing
        V previous = get(key);
        File f = generateFile(key);
        f.delete();
        return previous;
    }

    /**
     * Puts all the entries of the Map m into this Map
     * @param m Map whose entries are to be added to this Map
     * @throws IOException
     */
    public void putAll(Map<? extends K, ? extends V> m) throws IOException{
        //TODO: try to figure out how to make this atomic in case IOException
        //gets thrown in the middle of inserting the set
        Set<? extends K> keySet = m.keySet();
        for (K k: keySet){
            put(k, m.get(k));
        }
    }

    /**
     * Deletes all the entries in this map
     */
    public void clear(){
        for (File f : getFiles()){
            f.delete();
        }
    }

    /**
     * Searches the Map for the Value
     * @param value
     * @return true if the value occurs at least once, else false
     * @throws IOException
     */
    public boolean containsValue(Object value) throws IOException{
        for (File f: getFiles()){
            if (getEntry(f.getPath()).getValue().equals(value)){
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @return a collection of all the values in this Map
     * @throws IOException
     */
    public Collection<V> values() throws IOException{
        // TODO: I cheated by using an in-memory List.  This defeats the whole
        // point of FileSystemHashMap, becaue I may never be able to load this
        // whole list into memory.  I'm just using it here for now as a placeholder
        // until I implement some sort of FileSystemList that gives a view
        // of the HashMap
        
        Collection<V> values = new ArrayList<V>(getSize());
        for (File f: getFiles()){
            values.add(
                getEntry(f.getPath()).getValue()
            );
        }
        return values;
    }

    public Set<K> keySet() throws IOException{
        // TODO: cheated, see comment for values()
        Set<K> keySet = new HashSet<K>(getSize());
        for (File f: getFiles()){
            keySet.add(
                getEntry(f.getPath()).getKey()
            );
        }
        return keySet;
    }

    public Set<Entry<K, V>> entrySet() throws IOException{
        // TODO: cheated, see comment for values()
        Set<Entry<K,V>> entrySet = new HashSet<Entry<K,V>>(getSize());
        for (File f: getFiles()){
            entrySet.add(
                getEntry(f.getPath())
            );
        }
        return entrySet;
    }

    /****Private Helper Methods ****/

    /**
     * Generates a filename for key, essentially returns the String
     * representation of dataStore + key.hashCode()
     * @param key
     * @return
     */
    private String generateFilename(Object key){
        return generateFile(key).getPath();
    }
    /**
     * Returns
     * @param key
     * @return
     */
    private File generateFile(Object key){
        String filename = String.valueOf(key.hashCode());
        return new File(dataStore, filename);
    }

    /**
     *
     * @return an Array of all the files that Map uses to store Entries
     */
    private File[] getFiles(){
        return dataStore.listFiles();
    }

    private int getSize(){
        return dataStore.list().length;
    }

    /**
     * Given a filename, returns the key value pair stored inside it
     * @param filename
     * @return Entry representing the key value pair
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private Entry<K,V> getEntry(String filename) throws IOException{
        if (filename == null) return null;

        try{
            ObjectInputStream is = new ObjectInputStream(
                                        new FileInputStream(filename)
                                        );

            // TODO: decide if we should just inject the key passed in or
            // unserialize the key and put that in the entry 
            K k = (K) is.readObject();
            V value = (V) is.readObject();

            is.close();

            return new FileSystemHashMapEntry<K,V>(k,value);
            
            
        } catch (FileNotFoundException e){
            // this exception should not propogate up because if the file is not
            // found, that means that no such item is in the hash
            return null;
        } catch (ClassNotFoundException e){
            return null;
        }
    }

    /**
     * Subclass of SimpleEntry that forces Keys and Values to be Serializable.
     * Used by FileSystemHashMap
     * @param <K>
     * @param <V>
     */

    private static class FileSystemHashMapEntry<K extends Serializable, V extends Serializable >
            extends java.util.AbstractMap.SimpleEntry<K,V> {
        private static final long serialVersionUID = 1L;

        public FileSystemHashMapEntry(K key, V value) {
            super(key, value);
        }
    }
}