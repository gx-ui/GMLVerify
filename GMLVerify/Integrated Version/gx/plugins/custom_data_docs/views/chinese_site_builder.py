#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中文站点构建器
"""

from great_expectations.render.renderer.site_builder import SiteBuilder
from great_expectations.render.view.view import DefaultJinjaIndexPageView

class ChineseSiteIndexBuilder(SiteBuilder):
    """
    中文站点索引构建器
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def build(self, *args, **kwargs):
        """
        构建中文站点
        """
        # 调用父类的构建方法
        result = super().build(*args, **kwargs)
        
        # 这里可以添加中文化的逻辑
        print("正在构建中文数据文档站点...")
        
        return result

class ChineseIndexPageView(DefaultJinjaIndexPageView):
    """
    中文索引页面视图
    """
    
    def render(self, *args, **kwargs):
        """
        渲染中文索引页面
        """
        # 调用父类渲染方法
        result = super().render(*args, **kwargs)
        
        # 进行中文化处理
        if isinstance(result, str):
            # 替换一些常见的英文文本为中文
            chinese_translations = {
                'Data Docs': '数据文档',
                'Expectations': '期望',
                'Validations': '验证',
                'Profiling': '数据分析',
                'Overview': '概览',
                'Expectation Suites': '期望套件',
                'Validation Results': '验证结果',
                'Data Assets': '数据资产',
                'Last Updated': '最后更新',
                'Status': '状态',
                'Success': '成功',
                'Failed': '失败',
                'Partial Success': '部分成功'
            }
            
            for english, chinese in chinese_translations.items():
                result = result.replace(english, chinese)
        
        return result