package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.google.common.base.Objects;

import java.util.HashMap;
import java.util.Map;

public class StoreURL {
    private String id;
    private String name;
    private String graph;
    private String url;
    private String user;
    private String password;
    private boolean securityEnabled;
    private String userPrincipal;
    private Map<String, String> filePath = new HashMap<>();
    private String envVersion;
    private String storeVersion;


    public StoreURL( String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public StoreURL(String name, String url, String user, String password) {
        this.name = name;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreURL storeURL = (StoreURL) o;
        return securityEnabled == storeURL.securityEnabled &&
                Objects.equal(id, storeURL.id) &&
                Objects.equal(name, storeURL.name) &&
                Objects.equal(url, storeURL.url) &&
                Objects.equal(user, storeURL.user) &&
                Objects.equal(password, storeURL.password) &&
                Objects.equal(userPrincipal, storeURL.userPrincipal) &&
                Objects.equal(filePath, storeURL.filePath) &&
                Objects.equal(envVersion, storeURL.envVersion) &&
                Objects.equal(storeVersion, storeURL.storeVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, url, user, password, securityEnabled, userPrincipal, filePath, envVersion, storeVersion);
    }
}
