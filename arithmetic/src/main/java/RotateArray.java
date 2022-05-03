import java.sql.Array;
import java.util.Arrays;

/**
 * @Author：zhd
 * @Date: 2022/1/22 10:32
 * @Dscription: 旋转数组
 */
public class RotateArray {
    public static void main(String[] args) {

        int [] nums ={1,2,3,4,5,6,7};
        int k=2;

        //方法2传进去是数组的地址，修改后初始创建的也就修改了
//        roteate2(nums,k);


        roteate3(nums,k);

        System.out.println(Arrays.toString(nums));

    }

    /**
     * 方法一直接通过新的数组来实现原数组值的转换,最后在赋值给原数组
     * 方法的复杂度：Space is O(n) and time is O(n)
     * @param nums
     * @param k
     */
    public static void roteate1(int[] nums,int k){
        if (k>nums.length){
            k=k%nums.length;
        }
        System.out.println(k);

        //定义一个新的数组
        int[] result = new int[nums.length];
        //将后面的值赋到前面
        for (int i =0;i<k;i++){
            result[i] = nums[nums.length-k+i];
        }
        //将数组的k前数据放到前面
        int j=0;
        for (int i=k;i<nums.length;i++){
            result[i] =nums[j];
            j++;
        }
        System.arraycopy(result,0,nums,0,nums.length);
    };

    /**
     * 方法二通过每次遍历将k后面的放到前面
     * time is O(n*k).O(1) space
     * @param nums
     * @param k
     */
    public static  void roteate2 (int [] nums,int k){
        if (nums == null || k<0){
            throw new IllegalArgumentException("参数错误！！！");
        }
        //i每次加1就是将最后的元素变成第一个
        //嵌套for实现数组转换
        for(int i =0 ;i<k;i++){
            for (int j=nums.length-1;j>0;j--){
                int tmp =nums[j];
                nums[j]=nums[j-1];
                nums[j-1] =tmp;
            }
        }

    }

    /**
     * this in O(1) space and in O(n) time
     * @param nums
     * @param k
     */
    public static void roteate3 (int [] nums,int k){
        k=k%nums.length;
        if (nums ==null ||k<0){
            throw new IllegalArgumentException("参数错误！！！");
        }

        int a = nums.length - k;
        reverse(nums,0,a-1);
        reverse(nums,a,nums.length-1);
        reverse(nums,0,nums.length-1);

    }

    public static void reverse(int[] arr,int left,int right){
        if (arr==null||arr.length ==1){
            return;
        }
        while (left<right){
            int tmp = arr[left];
            arr[left]=arr[right];
            arr[right]=tmp;
            left++;
            right--;
        }
    }

}
