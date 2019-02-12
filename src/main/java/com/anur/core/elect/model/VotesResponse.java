package com.anur.core.elect.model;

/**
 * Created by Anur IjuoKaruKas on 1/25/2019
 *
 * 拉票结果
 */
public class VotesResponse extends Votes {

    /**
     * 拉票成功/失败
     */
    private boolean agreed;

    /**
     * 去拉票，结果拉到了leader节点，则无需继续拉票了，直接成为follower。
     */
    private boolean fromLeaderNode;

    /**
     * 请求拉票时的世代信息
     */
    private long askVoteGeneration;

    public VotesResponse() {
    }

    public VotesResponse(boolean agreed, boolean fromLeaderNode, long askVoteGeneration) {
        this.agreed = agreed;
        this.fromLeaderNode = fromLeaderNode;
        this.askVoteGeneration = askVoteGeneration;
    }

    public VotesResponse(long generation, String serverName, boolean agreed, boolean fromLeaderNode, long askVoteGeneration) {
        super(generation, serverName);
        this.agreed = agreed;
        this.fromLeaderNode = fromLeaderNode;
        this.askVoteGeneration = askVoteGeneration;
    }

    public boolean isAgreed() {
        return agreed;
    }

    public boolean isFromLeaderNode() {
        return fromLeaderNode;
    }

    public void setAgreed(boolean agreed) {
        this.agreed = agreed;
    }

    public void setFromLeaderNode(boolean fromLeaderNode) {
        this.fromLeaderNode = fromLeaderNode;
    }

    public long getAskVoteGeneration() {
        return askVoteGeneration;
    }

    public void setAskVoteGeneration(long askVoteGeneration) {
        this.askVoteGeneration = askVoteGeneration;
    }
}
