const getCategoryIdByName = async (name) => {
    try {
      const res = await api.get("products/categories", {
        per_page: 100
      });
  
      const found = res.data.find(cat => cat.name.toLowerCase() === name.toLowerCase());
      return found?.id || null;
    } catch (err) {
      console.error("🔍 Error fetching categories:", err.response?.data || err.message);
      return null;
    }
  };