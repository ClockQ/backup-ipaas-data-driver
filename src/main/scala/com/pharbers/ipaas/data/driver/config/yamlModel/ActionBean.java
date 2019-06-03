package com.pharbers.ipaas.data.driver.config.yamlModel;

import java.util.List;
import java.util.Map;

public class ActionBean {
    private String name;
    private String factory;
    private Map<String, String> args;
    private List<OperatorBean> opers;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFactory() {
        return factory;
    }

    public void setFactory(String factory) {
        this.factory = factory;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

    public List<OperatorBean> getOpers() {
        return opers;
    }

    public void setOpers(List<OperatorBean> opers) {
        this.opers = opers;
    }
}
