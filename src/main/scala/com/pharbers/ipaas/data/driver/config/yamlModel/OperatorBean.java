package com.pharbers.ipaas.data.driver.config.yamlModel;

import java.util.List;
import java.util.Map;

public class OperatorBean {
    private String name;
    private String factory;
    private String oper;
    private Map<String, String> args;
    private List<PluginBean> plugins;

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

    public String getOper() {
        return oper;
    }

    public void setOper(String oper) {
        this.oper = oper;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

    public List<PluginBean> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<PluginBean> plugins) {
        this.plugins = plugins;
    }
}
