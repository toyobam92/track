
import Button from 'react-bootstrap/Button';

const ThemeToggler = (props) => {


  const handleToggle = (e) => {
    props.setDark(!props.isDark);
    document.documentElement.setAttribute('data-theme', props.isDark ? 'light' : 'dark');
  };

  return (
    <div>
      <Button  style={{margin: "15px"}} onClick={handleToggle}>Dark Mode</Button>
    </div>
  );
};

export default ThemeToggler;
