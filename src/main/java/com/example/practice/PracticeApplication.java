package com.example.practice;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Stack;
@SpringBootApplication
public class PracticeApplication {

	public static void main(String[] args) {

		int n = 5;
		Stack<Integer> s = new Stack<>();
		while(n/2 > 1){
			n= n/2;
			s.push(n%2);
		}


		while(!s.empty())
			System.out.print(s.pop());

		System.out.println();
		System.out.print(Integer.bitCount(5));
		System.out.print(Integer.highestOneBit(5));
		System.out.print(Integer.lowestOneBit(5));
		System.out.print(Integer.reverse(143));

	}

}
