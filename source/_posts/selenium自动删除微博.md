     最近微博账号被盗用，莫名发了很多广告的微博。占据了前面几页，导致别人访问我的微博印象很不好。数了数大约前100条微博都是垃圾内容，如果手动一个一个删除会相当累。于是想用python实现一个自动删除微博内容的代码，搜了一下用selenium的webdriver模拟鼠标的点击删除操作是一个可行的方法。

​    代码比较简单，因为我这里模拟了chrome浏览器，所以要下载与本机chrome浏览器版本一致的chromedriver并放入chrome浏览器的安装路径。

```
from selenium import webdriver
import time


class WeiboCrawler:

    def __init__(self):
        self.chrome_driver = 'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe'
        self.browser = webdriver.Chrome(executable_path=self.chrome_driver)  ##控制谷歌浏览器
        self.browser.get('http://weibo.com/login.php')  ## 打开微博
        self.browser.maximize_window()

    def login(self, user, pasw, numb):
        self.browser.find_element_by_xpath('//*[@id="loginname"]').send_keys(user)
        # 把密码填入
        self.browser.find_element_by_xpath('//*[@id="pl_login_form"]/div/div[3]/div[2]/div/input').send_keys(pasw)
        # 把记住我勾去掉
        self.browser.find_element_by_xpath('//*[@id="login_form_savestate"]').click()

        # 点击登录按钮
        self.browser.find_element_by_xpath('//*[@id="pl_login_form"]/div/div[3]/div[6]/a').click()
        time.sleep(4)

        print("登陆成功")
        self.del_start(numb)

    def del_start(self, numb):
    #进入我的主页
        self.browser.find_element_by_xpath('//*[@id="v6_pl_rightmod_myinfo"]/div/div/div[2]/ul/li[3]/a/strong').click()

        for i in range(0, numb):
            time.sleep(3)
            # 下拉菜单
            self.browser.find_element_by_xpath(
                '//*[@id="Pl_Official_MyProfileFeed__20"]/div/div[2]/div[1]/div[1]/div/a/i').click()
            # 点击删除按键
            self.browser.find_element_by_xpath(
                '//*[@id="Pl_Official_MyProfileFeed__20"]/div/div[2]/div[1]/div[1]/div/div/ul/li[1]/a').click()
            # 点击确认删除
            self.browser.find_element_by_xpath("//*[@id]/div/p[2]/a[1]/span").click()
        self.browser.close()
        print('微博已经清空')


if __name__ == '__main__':
    starttime = time.time()
    weiboCrawler = WeiboCrawler()
    n = 20
    weiboCrawler.login("微博账号", "微博密码", n)
    endtime = time.time()
    dtime = endtime - starttime
    print('删除完成' + str(n) + '篇微博')
    print("程序运行时间：%.8s s" % dtime)
```

​    直接运行即可，主要就是模拟登录及拿到各个事件的标识并点击。有一个问题，点击登录按钮这里如果操作比较频繁，会弹出输入验证码的窗口，造成登录超时拿不到我的主页的标识。可以重复运行试一下，或者注释掉自动点击登录，多sleep几秒，然后手动点击登录窗并输入验证码。

运行结果如下，微博瞬间清静了很多：

> 登陆成功
> 微博已经清空
> 删除完成20篇微博
> 程序运行时间：75.53805 s