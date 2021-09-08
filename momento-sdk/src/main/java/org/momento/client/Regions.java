package org.momento.client;

/* Cloud and Regions supported by Momento */
public enum Regions {

    // TODO: Update the Endpoint URL
    AWS_US_WEST_2("cacheservice.us-west-2.momentohq.com");

    private final String endpoint;

    Regions(String endpoint) {
        this.endpoint = endpoint;
    }
}
