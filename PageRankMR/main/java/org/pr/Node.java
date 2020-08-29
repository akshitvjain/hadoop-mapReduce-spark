package org.pr;

public class Node {

    private String id;
    private Double pageRank;
    private String neighbor;
    private boolean isNode;

    public Node(String nodeInfo) {
        String[] info = nodeInfo.split("\t");
        if (info.length > 1) {
            this.id = info[0];
            this.neighbor = info[1];
            this.pageRank = Double.parseDouble(info[2]);
            this.isNode = true;
        }
        else {
            this.isNode = false;
            this.pageRank = Double.parseDouble(info[0]);
        }
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getPageRank() {
        return pageRank;
    }

    public void setPageRank(Double pageRank) {
        this.pageRank = pageRank;
    }

    public String getNeighbor() {
        return neighbor;
    }

    public void setNeighbor(String neighbor) {
        this.neighbor = neighbor;
    }

    public boolean isNode() {
        return isNode;
    }

    public void setNode(boolean node) {
        isNode = node;
    }
}
