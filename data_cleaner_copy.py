import pandas as pd
import numpy as np

class DataCleaner(object):
    # base class for cleaning dataframes
    def __init__(self, data):
        """Takes in list of dictionaries"""
        self._data = data
        self._dataframe = None
      
    @property
    def data(self):
        """Getter for the input data"""
        return self._data
    
    @data.setter
    def data(self, datum):
        """Setter for the data to clean"""
        self._data.extend(datum)
        
    @property
    def dataframe(self):
        """Getter for cleaned dataframe"""
        return self._dataframe
    
    @dataframe.setter
    def dataframe(self, df):
        """Setter for cleaned dataframe"""
        if type(df) == pd.core.frame.DataFrame:
            self._dataframe = df
        else:
            raise Exception("Must be a pandas dataframe!")
        
    def __change__(self, dataframe, change):
        """
        Takes in a dataframe as dataframe
        takes in a bool as change
        returns a copy of a dataframe
        """
        if change:
            self.dataframe = dataframe
            return self.dataframe
        else:
            return dataframe
            
    def parse_data(self, data, key):
        """
        Takes in a list of dictionaries as data
        a key in a dictionary as key
        returns a dataframe from the dictionaries in data
        """
        info = []
        for datum in data:
            if key in datum:
                info.extend(datum[key])
        self.dataframe = pd.DataFrame(info)
        return self.dataframe
    
    def view_only_certain_type(self, dataframe, datatype):
        """
        Takes in a dataframe as dataframe
        takes in a type class as datatype not as a string
        return a dataframe with only the datatype input
        """
        df = dataframe.copy()
        view = pd.DataFrame(df.dtypes)
        view = view[view[0] != datatype]
        view = df.drop(view.index, axis=1)
        return view
    
    def view_item_type(self, dataframe):
        """
        Takes in a dataframe as dataframe
        returns a dataframe with the type of each item in the dataframe
        """
        df = dataframe.copy()
        view = df.apply(lambda x: x.apply(type))
        return view
    
    def columns_to_break_down(self, dataframe, hide=False):
        """
        Takes in a dataframe as dataframe
        takes in an optional bool as hide
        returns a the names of the columns that contains the type in type_lst
        """
        breakdown = np.array([])
        checker = self.view_item_type(dataframe)
        type_lst = [list, dict]
        for arg in type_lst:
            # check the type with the dataframe with only types
            want = pd.DataFrame(checker[checker == arg].count())
            res = np.array(want[want[0] > 0].index)
            breakdown = np.union1d(breakdown, res)  
            
            # print the type and the columns if hide is False
            if not hide:
                print(arg)
                print(res, "\n")
        return breakdown
            
<<<<<<< HEAD
    def breakdown_list_types(self, data=None, flag=False):
        bd = self.columns_to_break_down([list], hide=True)
        df = self.__choose_df__(data)
        for col in bd:
            df[col] = df[col].apply(lambda x: list(np.array(x).flatten()))
        return self.__change__(df, flag)
   
    def collapse_feature(self, features, data, flag=False):
        df = self.__choose_df__(data)
<<<<<<< HEAD
=======
<<<<<<< HEAD
        df = self.breakdown_list_types(df, flag)
=======
>>>>>>> master
>>>>>>> cleaning_branch
=======
    def collapse_feature(self, dataframe, flag=False):
        """
        Takes in a dataframe as dataframe
        takes in a list of columns with list and dict entries as features
        takes in an optional bool as flag
        returns a dataframe with extra features extracted from the features
        """
        df = dataframe.copy()
        features = self.columns_to_break_down(dataframe, True)
>>>>>>> cleaning_branch
        for feature in features:
            try:
                # creates extra features from columns with list and dict entries
                feature_df = df[feature].apply(pd.Series)
                col = [f"{feature}_{i}" for i in range(0, len(feature_df.columns))]
                feature_df.columns =  col
                df = pd.concat([df, feature_df], axis = 1)
                df.drop([feature], axis=1, inplace=True)
            except:
                print(f"No feature: {feature}")
        return self.__change__(df, flag)
    
    def set_up_categories(self, dataframe, cate, flag=False):
        df = dataframe.copy()
        categories = [cat for cat in data.columns if cat.startswith(cate)]
        count = 0
        for category in categories:
            df_change = df[category].apply(pd.Series)
            df_change.drop([0], axis=1, inplace=True)
            df_change.columns = [f'{col}_{count}' for col in df_change.columns]
            count += 1
            df = pd.concat([df, df_change], axis=1)
        df.drop(categories, axis=1, inplace=True)
        return self.__change__(df, flag)
    
    def get_unique(self, cate, data):
        arr_comb_all = np.array([])
        cat_array = [cat for cat in data.columns if cat.startswith(cate)]
        for category in cat_array:
            arr_comb_all = np.union1d(data[category].unique().astype(str), arr_comb_all)
        return arr_comb_all
    
    def find_related_keywords(self, words, arr):
        lst = []
        for item in arr:
            for word in words:
                if word in item:
                    lst.append(item)
                    break
        return lst
        
    def sim_column_encoding(self, dataframe, related_keywords, cols, flag=False):
        count = 0
        columns = [cat for cat in dataframe.columns if cat.startswith(cols)]
        data = dataframe.copy()
        for col in columns:
            encoding = []
            for item in data[col].astype(str):
                flag = False
                for key in related_keywords:
                    if key in item:
                        encoding.append(1)
                        flag = True
                        break
                if flag == False:
                    encoding.append(0)
                else:
                    flag = False
            data[f"{cols}_{count}_code"] = encoding
            count += 1
        return self.__change__(data, flag)
    
    def rename_columns(self, df, col):
        df_work = df.copy().reset_index().drop(["index"], axis=1)
        df_work.columns = col
        df_work.drop([x for x in df_work.columns if "drop" in x], axis=1, inplace=True)
        return df_work
    
    def replace_vals(self, df, vals_to_replace, vals_to_replace_with):
        replace_dic = dict(zip(vals_to_replace, vals_to_replace_with))
        dfcopy = df.replace(replace_dic)
        return dfcopy
    
    def drop_columns(self, df, cols):
        dfcopy = df.copy()
        dfcopy = dfcopy.drop(cols, axis=1).reset_index()       
        return dfcopy.drop(["index"], axis=1)

class TutoringDataCleaner(DataCleaner):
    
    def __init__(self, data, complete=False):
        DataCleaner.__init__(self, data)
        if complete:
            self.dataframe = self.clean_entry()
        
    def parse(self, data):
        return self.parse_data(data, "businesses")
   
    def collapse(self, data, flag=False):
        df = self.collapse_feature(data)
        df["coordinates"] = tuple(zip(df.coordinates_0, df.coordinates_1))
        df.drop(["location_7", "distance", "image_url", "alias", "coordinates_0", 
                   "coordinates_1"], axis=1, inplace=True)
        return self.set_up_categories("categories", df, flag)
 
    def find_related_tutoring_keywords(self, data):
        arr = self.get_unique("title", data)
<<<<<<< HEAD
<<<<<<< HEAD
        hot_words = ["Tutor", "Child", "Kids", "Education", "Community", "Cultural", "Camps", 
                     "Preschool", "Clubs", "Prep", "Homework"]
=======
<<<<<<< HEAD
        hot_words = ["Tutor", "Education", "Community", "Camps", 
                     "Preschool", "Prep", "Homework", "Test"]
=======
        hot_words = ["Tutor", "Child", "Kids", "Education", "Community", "Cultural", "Camps", 
                     "Preschool", "Clubs", "Prep", "Homework"]
>>>>>>> master
>>>>>>> cleaning_branch
=======
        hot_words = ["Tutor", "Education", "Community", "Camps", 
                     "Preschool", "Prep", "Homework", "Test"]
>>>>>>> cleaning_branch
        return self.find_related_keywords(hot_words, arr)
    
    def title_encoding(self, data, flag=False):
        related_keywords = self.find_related_tutoring_keywords(data)
        df = self.sim_column_encoding(data, related_keywords, "title", flag)
        df["counts"] = 0
        for col in df.columns:
            if "code" in col:
                df["counts"] += df[col]
        return self.__change__(df, flag)
    
    def find_tutoring(self, data, flag=True):
        try:
            df = data.copy()[data.counts.values > 0]
            df.reset_index(inplace=True)
            df.drop(["index"], axis=1, inplace=True)
            return self.__change__(df, flag)
        except:
            raise Exception("Must a encode the titles in the dataframe and create a count column!")
            
    def clean_entry(self):
        df = self.collapse(self.parse())
        df = self.find_tutoring(self.title_encoding(df))
        return df
    
class ReviewDataCleaner(DataCleaner):
    
    def __init__(self, raw_review_info):
        DataCleaner.__init__(self, raw_review_info)
        self._raw_review_info = raw_review_info
        self._review_df = pd.DataFrame()
        
    @property
    def review_df(self):
        return self._review_df
    
    @review_df.deleter
    def review(self):
        self._review_df = pd.DataFrame()
    
    @property
    def raw_review_info(self):
        return self._raw_review_info
    
    def clean_review(self, info):
        cols = ["business_name", "user_name", "user_location","date", "review", "star_rating", "user_stats"]
        keys = list(info.keys())
        vals = list(info.values())
        data = []
        for i in range(len(vals)):
            for j in range(len(vals[i]["User"])):
                if len(vals[i]["User"][j]) > 1:
                    data.append([keys[i], vals[i]["User"][j][0],  vals[i]["User"][j][1], vals[i]["Rev_Date"][j][:10], 
                             vals[i]["User_Rev"][j], vals[i]["Star_Rating"][j], vals[i]["User_Stat"][j]])
                else:
                    data.append([keys[i], vals[i]["User"][j][0],  np.nan, vals[i]["Rev_Date"][j][:10], 
                             vals[i]["User_Rev"][j], vals[i]["Star_Rating"][j], vals[i]["User_Stat"][j]])
        user_df = pd.DataFrame(data, columns=cols)
        return user_df
    
    def clean_all_reviews(self, info):
        revs_df = pd.DataFrame()
        stats_col_name = ["friend_count", "review_count", "photo_count", "elite_status"]
        for dic in info:
            revs_df = pd.concat([revs_df, self.clean_review(dic)], axis=0, sort=True, ignore_index=True)
        revs_df = self.collapse_feature(["user_stats"], revs_df) 
        revs_df.rename(columns=dict(zip([f"user_stats_{i}" for i in range(4)], stats_col_name)), inplace=True)
        return revs_df
    
    def organize_reviews(self, df):
        name = ""
        count = 0
        new_rev = pd.DataFrame()
        for _, row in df.iterrows():
            if name != row.business_name+f"{count}":
                count += 1
                row.business_name = row.business_name + f"{count}"
                name = row.business_name
                new_rev = pd.concat([new_rev, pd.DataFrame(row)], axis=1)
            else:
                row.business_name = row.business_name + f"{count}"
                new_rev = pd.concat([new_rev, pd.DataFrame(row)], axis=1)
        new_rev = new_rev.T.drop(["date"], axis=1)
        return new_rev
    

class AreaDataCleaner(DataCleaner):
    
    def __init__(self, df):
        DataCleaner.__init__(self, df)
        
    def clean_living_data(self, df):
        vals_to_replace =  ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "F"]
        dfcopy = self.replace_vals(df, vals_to_replace, list(range(1, 14)[::-1]))
        dfcopy["zipcode"] = dfcopy["zipcode"].astype(str)
        all_ = pd.DataFrame()
        for num, rows in self.data.iterrows():
            if rows.location_4 in dfcopy.zipcode.values:
                all_= pd.concat([all_, dfcopy[dfcopy.zipcode == rows["location_4"]]], axis=0)
            else:
                first = np.array([[np.nan]*8+[rows.location_4]])
                all_ = pd.concat([all_, pd.DataFrame(first, columns=dfcopy.columns)], axis=0)

        all_ = all_.reset_index().drop(["index"], axis=1)
        return all_
               
    def rename_area_df_cols(self, df):
        dem_col = ['1k_to_1499_rent', '1.5k_to_1,999_rent', '150k_plus_salary',
       '2k_plus_rent', '30k_to_74,999_salary', '600_to_999_rent',
       '75k_to_149,999_salary', 'drop_1', '1_person_household', 'drop_2',
       'age_range_10_to_17', 'age_range_18_to_24', 'drop_3', '2_to_3_person_household',
       'drop_4', 'age_range_25_to_39', 'drop_5', 'drop_6',
       '4-5 Person', 'drop_7', '40 to 64 Years Old', 'drop_8',
       '6 or More Person', '65 Years Old or Over', 'drop_9',
       '9 Years Old or Under', 'drop_10',
       'drop_10', 'drop_11',
       'bachelors_or_associate_degree', 'drop_12',
       'drop_13', 'drop_14', 'drop_15',
       'drop_16', 'employed_armed _forces', 'employed_civilian',
       'drop_17', 'graduate_degree', 'high_school_graduate',
       'high_school_or_less', 'drop_18', '30k_less_salary',
       'drop_19', 'drop_20', 'drop_21', 'not_in_labor_force',
       'drop_22', 'owner_occupied_home', 'private_vehicle', 'drop_23',
       'drop_24', 'public_transportation', 'renter_occupied_home',
       'drop_25', 'taxi', 'drop_26', 'unemployed',
       'walk_or_bicycle', 'work_from_home', 'zipcode']
        return self.rename_columns(df, dem_col)
        
    def clean_area_dem(self, df):
        df_work = self.rename_area_df_cols(df)
        df_work = df_work.astype(str)
        for num, row in df_work.iterrows():
            for x in df_work.columns[:-1]:
                if row[x] != "nan" or row[x] != None:
                    row[x] = row[x].split(" ")
                    if len(row[x]) > 1:
                        row[x] = float(row[x][1].strip("(%)"))*.01
                    else:
                        row[x] = float(row[x][0])
        return df_work


    
    
    
    
    
    
    
    
    
