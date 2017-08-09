package com.example;


import java.io.Serializable;

/* Java bean to convert the Movies RDD to Dataframe*/

public class Movie implements Serializable{
    private Integer movieId;
    private String name;
    private String genres;

    public String getName(){
        return this.name;
    }
    public void setName(String name){
        this.name = name;
    }

    public Integer getMovieId(){
        return this.movieId;
    }
    public void setMovieId(Integer id){
        this.movieId = id;
    }

    public String getGenres(){
        return genres;
    }

    public void setGenres(String genres){
        this.genres = genres;
    }

    @Override
    public String toString(){
        return "Movie(" + this.movieId + ", " + this.name + ", " + this.genres + ")";
    }
}
