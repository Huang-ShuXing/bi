package com.maywide.bi.core.controller;

import com.maywide.bi.core.services.JobServices;
import com.maywide.bi.core.services.TestServices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    /*@Autowired
    private TestServices testServices;
    @Autowired
    private JobServices jobServices;

    @RequestMapping("/one")
    public String testOne(){
        return testServices.testOneService();
    }

    @RequestMapping("/insert")
    public String testIn (){
        testServices.testInsert();
        return "";
    }
    @RequestMapping("/testJob")
    public String testJob(){
        jobServices.excute();
        return "123";
    }
    @RequestMapping("/testOneJob/{id}")
    public int testJob(@PathVariable("id") int id){
        jobServices.runJobById(id);
        return id ;
    }*/
}
