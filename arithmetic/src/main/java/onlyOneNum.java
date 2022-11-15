import java.util.Arrays;
import java.util.List;

/**
 * @Author：zhd
 * @Date: 2022/11/4 13:47
 * @Dscription:
 */
public class onlyOneNum {

    public static void main(String[] args) {

        //测试算法一
        int [] nums = {5,0,0,1,1,2,2,3,3,4,4};
        int a = only1Num(nums);
        System.out.println(a);



    }

    /**
     * 给定一个非空整数数组，除了某个元素只出现一次以外，其余每个元素均出现两次。找出那个只出现了一次的元素。
     * @param nums
     * @return 返回对应的值
     *
     * 使用异或的方式，如果把所有的数都异或，相同的数字异或为0，最后只剩下出现一次的数字，它和0异或，结果就是它本身。
     */
    public static int only1Num (int[] nums){
        int num = 0;
        for(int i = 0; i < nums.length; i++){
            num = num ^ nums[i];
            System.out.println(num);
        }
        return num;
    }






}
