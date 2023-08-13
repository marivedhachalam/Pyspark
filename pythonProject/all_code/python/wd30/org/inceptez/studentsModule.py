class Students:
    #@staticmethod
    def attendence(a,stud_list):
        return len(stud_list)
    def abcentees(a,a_stud_list):
        return len(a_stud_list)
    def sorted_list(a,a_stud_list):
        return sorted(a_stud_list)
    def max_time_login(a,login_time_list):
        return max(login_time_list)


o=Students()
print(o.attendence(['a','b','c']))
#o=Students()
#print(o.max_time_login([1,2,3]))
