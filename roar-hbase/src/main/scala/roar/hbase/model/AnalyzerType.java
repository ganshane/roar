// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
/*
 * Copyright 2012 The EGF IT Software Department.
 */
package roar.hbase.model;

/**
 * 分析器类型
 *
 * @author jcai
 */
public enum AnalyzerType {
    Standard,
    SmartCN,
    CJK;

    public Class clazz() {
        try {
            switch (ordinal()) {
                case 0:
                    return Class.forName("org.apache.lucene.analysis.standard.StandardAnalyzer");
                case 1:
                    return Class.forName("org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer");
                case 2:
                    return Class.forName("org.apache.lucene.analysis.cjk.CJKAnalyzer");
                default:
                    throw new IllegalStateException("Wrong class");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
