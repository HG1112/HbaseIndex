package com.unicrawl.controller;

import com.unicrawl.LucIndexing;
import com.unicrawl.Output;
import com.unicrawl.mr.Search;

import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Controller
public class HomeController {
    List<Output> results = new ArrayList<>();
    // List<Output> mrResults = new ArrayList<>();
    @GetMapping("/home")
    public String home(@RequestParam(required = false) String query, Model model) throws IOException, ParseException, InterruptedException {
        // LucIndexing lin = new LucIndexing();
        // results = lin.search(query);
        Search search = new Search(query, "/home/karna/projects/HbaseIndex/searchmr/target/HbaseIndex-1.0.0-jar-with-dependencies.jar");
        System.out.println("Query :" + query);
        results = search.run();
	System.out.println("Results size :" + results.size());
        model.addAttribute("query", query);
        return "redirect:/results";
//        return "home_page";
    }
    @GetMapping("/results")
    public String results(Model model){
        model.addAttribute("results", results);
        return "query_results";
    }
}
