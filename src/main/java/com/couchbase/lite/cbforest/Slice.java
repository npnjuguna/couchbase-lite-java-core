/* ----------------------------------------------------------------------------
 * This file was automatically generated by SWIG (http://www.swig.org).
 * Version 3.0.2
 *
 * Do not make changes to this file unless you know what you are doing--modify
 * the SWIG interface file instead.
 * ----------------------------------------------------------------------------- */

package com.couchbase.lite.cbforest;

public class Slice {
  private long swigCPtr;
  protected boolean swigCMemOwn;

  protected Slice(long cPtr, boolean cMemoryOwn) {
    swigCMemOwn = cMemoryOwn;
    swigCPtr = cPtr;
  }

  protected static long getCPtr(Slice obj) {
    return (obj == null) ? 0 : obj.swigCPtr;
  }

  protected void finalize() {
    delete();
  }

  public synchronized void delete() {
    if (swigCPtr != 0) {
      if (swigCMemOwn) {
        swigCMemOwn = false;
        cbforestJNI.delete_Slice(swigCPtr);
      }
      swigCPtr = 0;
    }
  }

  public static Slice getNull() {
    long cPtr = cbforestJNI.Slice_Null_get();
    return (cPtr == 0) ? null : new Slice(cPtr, false);
  }

  public Slice() {
    this(cbforestJNI.new_Slice__SWIG_0(), true);
  }

  public Slice(byte[] b) {
    this(cbforestJNI.new_Slice__SWIG_1(b), true);
  }

  public byte[] getBuf() {
    return cbforestJNI.Slice_getBuf(swigCPtr, this);
  }

  public int compare(Slice arg0) {
    return cbforestJNI.Slice_compare(swigCPtr, this, Slice.getCPtr(arg0), arg0);
  }

  public Slice copy() {
    long cPtr = cbforestJNI.Slice_copy(swigCPtr, this);
    return (cPtr == 0) ? null : new Slice(cPtr, false);
  }

  public void free() {
    cbforestJNI.Slice_free(swigCPtr, this);
  }

  public long getSize() {
    return cbforestJNI.Slice_getSize(swigCPtr, this);
  }

}