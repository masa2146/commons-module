package io.hubbox.commons;

import spark.Service;

import java.io.Serializable;

/**
 * @author fatih
 */
public interface ModuleCommons extends Serializable {

    long serialVersionUID = 10000000023L;

    /**
     * Call spark methods
     *
     * @param http spark http/https service
     */
    void onStart(Service http);

    /**
     * Call any method in this function
     */
    void onStart();

    /**
     * Declare fields in this method.
     */
    void onInit();

    /**
     * Call this method when jar removed
     */
    void onDestroy();

    /**
     * When stopped jar activity then call this method
     */
    void onStop();


}
