import java.util.Stack;

/**
 * @Author：zhd
 * @Date: 2022/1/24 20:50
 * @Dscription: 求取有一数组的运算符和数值的计算结果
 */
public class ReversePolishNotation {
    public static void main(String[] args) {

        String [] tokens = new String[]{"2", "1", "+", "3", "*"};
        System.out.println(evalRPN(tokens));


    }
    public static int evalRPN(String [] tokens){
        int returnValue = 0;
        String operators ="+-*/";

        Stack<String> stack = new Stack<>();

        for (String s : tokens) {
            if (!operators.contains(s)){
                stack.push(s);
            }else {
                int a = Integer.valueOf(stack.pop());
                int b = Integer.valueOf(stack.pop());
                //方法二 int index = operators.indexOf(t);
                //switch(index){ case 0:    stack.push(String.valueOf(a+b));
                //                        break;}
                switch (s){
                    case "+":
                        stack.push(String.valueOf(a+b));
                        break;
                    case"-":
                        stack.push(String.valueOf(a-b));
                        break;
                    case"/":
                        stack.push(String.valueOf(a/b));
                        break;
                    case"*":
                        stack.push(String.valueOf(a*b));
                        break;
                }
            }
        }
        returnValue = Integer.valueOf(stack.pop());

        return returnValue;
    }

}

