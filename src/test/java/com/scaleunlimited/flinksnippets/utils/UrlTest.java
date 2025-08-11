package com.scaleunlimited.flinksnippets.utils;

import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlTest {

    @Test
    public void testFragmentResolution() throws MalformedURLException {
        URL baseUrl = new URL("http://domain.com/somepage.html");
        URL url = new URL(baseUrl, "");
        System.out.println(url);
    }
}
