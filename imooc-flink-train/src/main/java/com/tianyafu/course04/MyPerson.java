package com.tianyafu.course04;

import java.util.Objects;

public class MyPerson {

    private String name;


    private String sex;

    private String school;

    public MyPerson() {
    }

    public MyPerson(String name, String sex, String school) {
        this.name = name;
        this.sex = sex;
        this.school = school;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyPerson myPerson = (MyPerson) o;
        return Objects.equals(name, myPerson.name) &&
                Objects.equals(sex, myPerson.sex) &&
                Objects.equals(school, myPerson.school);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, sex, school);
    }

    @Override
    public String toString() {
        return "MyPerson{" +
                "name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", school='" + school + '\'' +
                '}';
    }
}
