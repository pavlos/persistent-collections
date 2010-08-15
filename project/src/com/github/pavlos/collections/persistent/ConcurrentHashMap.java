package com.github.pavlos.collections.persistent;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentHashMap<K extends Serializable, V extends Serializable>
        implements IOMap<K ,V> {

    private final IOMap<K,V> map;
    private final int concurrency;
    private final ReadWriteLock[] locks;
    // a lock on the whole instance to avoid contention among threads for other locks
    private final ReadWriteLock instanceLock;
    // should we bother locking for reads?  setting this to false will sacrifice
    // consistency for concurrency
    private final boolean lockOnReads = true;
    // should we bother locking for writes?  setting this to false will sacrifice
    // consistency for concurrency, probably dangerous!
    private final boolean lockOnWrites = true;

    // try to estimate a reasonable level of concurrency
    protected static int determineConcurrencyLevel(){
        return Runtime.getRuntime().availableProcessors() * 10;
    }

    public ConcurrentHashMap(IOMap<K,V> map, int concurrency){
        this.map = map;
        // make sure there's at least one lock so modulus-0 isn't being attempted
        this.concurrency = (concurrency < 1) ? 1: concurrency;


        locks = new ReentrantReadWriteLock[concurrency];
        for(int i = 0; i < concurrency; i++){
            locks[i] = new ReentrantReadWriteLock(true); // true = fair locks
        }

        this.instanceLock = new ReentrantReadWriteLock(true);
    }

    public ConcurrentHashMap(IOMap<K,V> map){
       // TODO: figure out if one lock per thread is ideal. might want to 
       // dynamically create locks on a per-entry basis so that theres
       // no needless waiting on locks
       this(map, determineConcurrencyLevel());
    }

    public int size() {
        // TODO: once size is cached in FileSystemHashMap, we can eliminate
        // the need to acquire all the locks
        try{
            acquireAllReadLocks();
            return map.size();
        } finally {
            releaseAllReadLocks();
        }
    }

    public V remove(Object key) throws IOException {
        ReadWriteLock l = getLock(key);
        l.writeLock().lock();
        try{
            return map.remove(key);
        } finally {
            l.writeLock().unlock();
        }
    }

    public V put(K key, V value) throws IOException {
        ReadWriteLock l = getLock(key);
        l.writeLock().lock();
        try{
            return map.put(key, value);
        } finally {
            l.writeLock().unlock();
        }
    }

    public V get(Object key) throws IOException {
        ReadWriteLock l = getLock(key);
        l.readLock().lock();
        try{
            return map.get(key);
        } finally{
            l.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        // TODO: should we even bother locking for such a simple method?
        try{
            acquireAllReadLocks();
            // do our business
            return map.isEmpty();
        } finally {
            releaseAllReadLocks();
        }
    }

    public boolean containsKey(Object key) {
        try{
            acquireAllReadLocks();
            return map.isEmpty();
        } finally {
            releaseAllReadLocks();
        }
    }

    public synchronized void clear() {
        try{
            acquireAllWriteLocks();
            map.clear();
        } finally {
            releaseAllWriteLocks();
        }
    }

    public void putAll(Map<? extends K, ? extends V> m) throws IOException {

        // build a list of the indicies of the locks needed to perform
        // this operation, then acquire all those locks
        Set<Integer> need = new HashSet<Integer>(m.size());
        for(Object o: m.keySet()){
            need.add(this.getLockIndex(o));
        }
        try{
            acquireWriteLocks(need);
            map.putAll(m);
        } finally{
            releaseAllWriteLocks();
        }
    }

    public boolean containsValue(Object value) throws IOException {
        try{
            acquireAllReadLocks();
            return map.containsValue(value);
        } finally {
            releaseAllReadLocks();
        }
    }

    public Set<Entry<K, V>> entrySet() throws IOException {
        try{
            acquireAllReadLocks();
            return map.entrySet();
        } finally {
            releaseAllReadLocks();
        }
    }

    public Set<K> keySet() throws IOException {
        try{
            acquireAllReadLocks();
            return map.keySet();
        } finally {
            releaseAllReadLocks();
        }
    }

    public Collection<V> values() throws IOException {
        try{
            acquireAllReadLocks();
            return map.values();
        } finally {
            releaseAllReadLocks();
        }
    }


    /**** Private Helper Methods ****/

    /**
     * Get the lock on the specific key we need
     * @param key lock for this key
     * @return
     */
    private ReadWriteLock getLock(Object key){
        // take the absolute value of hashCode and mod it by concurrency to
        // determine which lock protects this entry
        int i = getLockIndex(key);
        return locks[i];
    }

    /**
     * Just get the index of the lock needed
     * @param key
     * @return index of lock needed for this key
     */
    private int getLockIndex(Object key){
        // take the absolute value of hashCode and mod it by concurrency to
        // determine which lock protects this entry
        return Math.abs(key.hashCode()) % concurrency;
    }

    /**
     * acquire every single read lock.  This is useful for building collection
     * based on the Map
     */
    private void acquireAllReadLocks(){
        
        // if we're not locking on reads, return early, don't bother
        if (!this.lockOnReads) return;

        // get the lock on the whole instance go avoid deadlocking
        instanceLock.readLock().lock();

        //build a set with the index of all the locks we need
        Set<Integer> need = new HashSet<Integer>(concurrency);
        for(int i = 0; i < concurrency; i++){
            need.add(i);
        }

        // and empty set with the locks we have
        Set<Integer> have = new HashSet<Integer>(concurrency);

        // while we still need locks
        while (need.size() > 0) {
            for(int i: need){
                // try to get every lock we need, without waiting on any of them
                // if we got the lock, add it to the list of locks we have
                // if we skip a lock, we'll get it on the next go-round
                if(locks[i].readLock().tryLock()){
                    have.add(i);
                }
            }
            // remove the locks we have from the locks we need
            need.removeAll(have);
        }
    }

    /**
     * release all read locls
     */
    private void releaseAllReadLocks(){
        // if we're not locking on reads, return early, don't bother
        if (!this.lockOnReads) return;
        // unlock all our locks
        for (int i=0; i < concurrency; i++){
            locks[i].readLock().unlock();
        }

        // release the lock on the instance
        instanceLock.readLock().unlock();
    }

    private void acquireAllWriteLocks(){
        //build a set with the index of all the locks we need
        Set<Integer> need = new HashSet<Integer>(concurrency);
        for(int i = 0; i < concurrency; i++){
            need.add(i);
        }
        this.acquireWriteLocks(need);
    }


    private void acquireWriteLocks(Set<Integer> need){

        // if we're not locking on writes, return early, don't bother
        if (!this.lockOnWrites) return;

        // get the lock on the whole instance go avoid deadlocking
        instanceLock.writeLock().lock();

        // and empty set with the locks we have
        Set<Integer> have = new HashSet<Integer>(concurrency);

        // while we still need locks
        while (need.size() > 0) {
            for(int i: need){
                // try to get every lock we need, without waiting on any of them
                // if we got the lock, add it to the list of locks we have
                // if we skip a lock, we'll get it on the next go-round
                if(locks[i].writeLock().tryLock()){
                    have.add(i);
                }
            }
            // remove the locks we have from the locks we need
            need.removeAll(have);
        }
    }

    /**
     * release all write locls
     */
    private void releaseAllWriteLocks(){
        // if we're not locking on writess, return early, don't bother
        if (!this.lockOnWrites) return;
        // unlock all our locks
        for (int i=0; i < concurrency; i++){
            locks[i].writeLock().unlock();
        }

        // release the lock on the instance
        instanceLock.writeLock().unlock();
    }
}