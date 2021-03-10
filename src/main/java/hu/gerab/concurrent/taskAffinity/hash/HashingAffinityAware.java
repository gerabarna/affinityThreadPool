package hu.gerab.concurrent.taskAffinity.hash;

import hu.gerab.concurrent.taskAffinity.AffinityAware;

public interface HashingAffinityAware extends AffinityAware<Object> {

    long getAffinityKey();
}
