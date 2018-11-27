package com.maywide.bi.core.execute;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OperationFactory {
    private static final  int FULL = 1;
    private static final  int INCR = 0;
    private static final  int GENERATE_INDEX = 2;

    @Autowired
    private FullDataOperation fullDataOperation;
    @Autowired
    private IncrDataOperation incrDataOperation;
    @Autowired
    private GenerateIndexOperation generateIndexOperation;

    public Operation getOperation(Integer way){
        switch(way) {
            case FULL:
                return fullDataOperation;
            case INCR:
                return incrDataOperation;
            case GENERATE_INDEX:
                return generateIndexOperation;
            default:
                return null;
        }
    }
}
