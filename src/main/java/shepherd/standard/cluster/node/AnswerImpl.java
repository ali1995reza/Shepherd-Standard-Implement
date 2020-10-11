package shepherd.standard.cluster.node;

import shepherd.api.asynchronous.AsynchronousResultListener;
import shepherd.api.message.Answer;
import shepherd.api.message.MessageMetadata;
import shepherd.api.message.Response;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

final class AnswerImpl<T> implements Answer<T> {


    private int numberOfPossibleResponses;
    private int numberOfRequiredResponses;
    private int numberOfFailedNodes;
    private int totalResponses;
    private int validResponses;
    private HashMap<Integer  ,  Response<T>> responses;
    private Map<Integer , Response<T>> unmodifiableResponses;
    private boolean timedOut ;
    private MessageMetadata questionMetadata;
    private TimerTask timeoutTask;
    private final AsynchronousResultListener<Answer<T>> listener;

    AnswerImpl(AsynchronousResultListener<Answer<T>> listener  , int nopr , int nonr)
    {
        this.listener = listener;
        numberOfPossibleResponses = nopr;
        numberOfRequiredResponses = nonr;
        responses = new HashMap<>(numberOfPossibleResponses);
        unmodifiableResponses = Collections.unmodifiableMap(responses);
        if(numberOfRequiredResponses==0)
        {
            callOnComplete();
        }
    }

    AnswerImpl(AsynchronousResultListener<Answer<T>> listener)
    {
        this.listener = listener;
    }

    /*void setNumberOfPossibleResponses(int nopr)
    {
        numberOfPossibleResponses = nopr;
        numberOfRequiredResponses = nopr;
        responses = new HashMap<>(numberOfPossibleResponses);
    }*/


    void setNumberOfPossibleResponses(int nopr , int nonr)
    {
        numberOfPossibleResponses = nopr;
        numberOfRequiredResponses = nonr;
        responses = new HashMap<>(numberOfPossibleResponses);
        unmodifiableResponses = Collections.unmodifiableMap(responses);
        if(numberOfRequiredResponses==0)
        {
            callOnComplete();
        }
    }




    boolean addFailedNodes(int i)
    {

        if(i<1)
            return false;


        //this is never must happen really !
        if(totalResponses == numberOfPossibleResponses)
            return false;

        if(timedOut)
            return false;
        totalResponses+=i;

        numberOfFailedNodes+=i;


        if (totalResponses != numberOfPossibleResponses) {
            callOnUpdate();
            return false;
        }

        callOnComplete();

        return true;

    }

    boolean putNewResponse(Response<T> resp)
    {

        assertIfResponsesDone();
        if(timedOut)
            return false;

        ++totalResponses;
        responses.put(resp.metadata().sender().id(), resp);
        if(!resp.hasError())++validResponses;


        if(validResponses!= numberOfRequiredResponses
                && totalResponses != numberOfPossibleResponses) {
            callOnUpdate();
            return false;
        }

        callOnComplete();
        return true;
    }


    @Override
    public Map<Integer, Response<T>> responses() {
        return unmodifiableResponses;
    }


    private void assertIfResponsesDone()
    {
        if(totalResponses == numberOfPossibleResponses
                || validResponses== numberOfRequiredResponses)
        {
            throw new IllegalStateException("all responses received");
        }
    }

    @Override
    public int numberOfFailedNodes() {
        return numberOfFailedNodes;
    }

    @Override
    public int numberOfRequiredResponses() {
        return numberOfRequiredResponses;
    }

    boolean setTimedOut()
    {
        if(totalResponses == numberOfPossibleResponses)
        {
            //so its done
            return false;
        }

        timedOut = true;

        callOnComplete();
        return true;
    }

    @Override
    public boolean timedOut() {
        return timedOut;
    }

    @Override
    public MessageMetadata questionMetadata() {
        return questionMetadata;
    }

    void setQuestionMetadata(MessageMetadata metadata)
    {
        questionMetadata = metadata;
    }

    public void setTimeoutTask(TimerTask timeoutTask) {
        this.timeoutTask = timeoutTask;
    }

    public TimerTask timeoutTask() {
        return timeoutTask;
    }

    private final void callOnComplete()
    {
        try {
            listener.onCompleted(this);
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    private final void callOnUpdate()
    {
        try {
            listener.onUpdated(this);
        }catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        String answerStr = "[ answers : \n { \n";
        for(Response<T> response : responses.values())
        {
            if(response.hasError())
                answerStr+="   "+response.metadata().sender() + " : ERROR - " + response.error() + " , \n";
            else
                answerStr+="   "+response.metadata().sender() + " : " + response.data()+ " , \n";

        }
        answerStr+=" } , \n";
        answerStr+=" TimedOut : "+timedOut+" ,";
        answerStr+=" RequiredResponses : "+ numberOfRequiredResponses +" ,";
        answerStr+=" Failed-Nodes : "+numberOfFailedNodes+" ]";

        return answerStr;
    }
}
