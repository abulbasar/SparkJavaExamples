package com.example;

/* Java bean to convert Ratings RDD to Dataframe*/
public class MovieRating {
    private Integer userId;
    private Integer movieId;
    private  Float rating;

    public Integer getUserId(){
        return this.userId;
    }
    public void setUserId(Integer value){
        this.userId = value;
    }

    public Integer getMovieId(){
        return this.movieId;
    }
    public void setMovieId(Integer value){
        this.movieId = value;
    }

    public Float getRating(){
        return this.rating;
    }

    public void setRating(Float value){
        this.rating = value;
    }

    @Override
    public String toString(){
        return "MovieRating(" + this.movieId + ", " + this.userId + ", " + this.rating  + ")";
    }
}
