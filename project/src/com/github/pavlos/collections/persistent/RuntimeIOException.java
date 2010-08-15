/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.github.pavlos.collections.persistent;

/**
 *
 * @author Paul Hieromnimon
 */
public class RuntimeIOException extends RuntimeException {

    public RuntimeIOException(Throwable cause) {
        super(cause);
    }

    public RuntimeIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public RuntimeIOException(String message) {
        super(message);
    }

    public RuntimeIOException() {
    }

}
