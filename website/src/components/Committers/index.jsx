import React from 'react'
import styles from './index.module.css'

// Sorted by apacheId
const committers = [
  { githubId: 'ClSlaid', apacheId: 'cailue', name: 'Cai Lue', inPMC: true },
  { githubId: 'wcy-fdu', apacheId: 'congyi', name: 'Congyi Wang', inPMC: false },
  { githubId: 'Young-Flash', apacheId: 'dongyang', name: 'Dongyang Zheng', inPMC: false },
  { githubId: 'G-XD', apacheId: 'gxd', name: 'Xiangdong', inPMC: true },
  { githubId: 'Hexiaoqiao', apacheId: 'hexiaoqiao', name: 'Xiaoqiao He', inPMC: true },
  { githubId: 'oowl', apacheId: 'junouyang', name: 'Jun Ouyang', inPMC: true },
  { githubId: 'WenyXu', apacheId: 'kangkang', name: 'Wenkang Xu', inPMC: false },
  { githubId: 'dqhl76', apacheId: 'liuqingyue', name: 'Liuqing Yue', inPMC: false },
  { githubId: 'Zheaoli', apacheId: 'manjusaka', name: 'Zheao Li', inPMC: true },
  { githubId: 'messense', apacheId: 'messense', name: 'Lusheng Lyu', inPMC: false },
  { githubId: 'morristai', apacheId: 'morristai', name: 'Morris Tai', inPMC: false },
  { githubId: 'WillemJiang', apacheId: 'ningjiang', name: 'Willem Ning Jiang', inPMC: true },
  { githubId: 'PsiACE', apacheId: 'psiace', name: 'Chojan Shang', inPMC: true },
  { githubId: 'silver-ymz', apacheId: 'silver', name: 'Mingzhuo Yin', inPMC: true },
  { githubId: 'sundy-li', apacheId: 'sundyli', name: 'Sundy Li', inPMC: true },
  { githubId: 'suyanhanx', apacheId: 'suyanhanx', name: 'Han Xu', inPMC: true },
  { githubId: 'tedliu1', apacheId: 'tedliu', name: 'Ted Liu', inPMC: true },
  { githubId: 'tisonkun', apacheId: 'tison', name: 'Zili Chen', inPMC: true },
  { githubId: 'wu-sheng', apacheId: 'wusheng', name: 'Sheng Wu', inPMC: true },
  { githubId: 'Xuanwo', apacheId: 'xuanwo', name: 'Hao Ding', inPMC: true },
  { githubId: 'Ji-Xinyou', apacheId: 'xyji', name: 'Xinyou Ji', inPMC: false }
]

export default function Committers() {
  return <>
    <table>
      <thead>
      <tr>
        <th><b>Avatar</b></th>
        <th><b>Name</b></th>
        <th><b>Apache ID</b></th>
        <th><b>GitHub ID</b></th>
      </tr>
      </thead>
      <tbody>
      {committers
        .sort((c0, c1) => c0.apacheId.localeCompare(c1.apacheId))
        .map(v => (
          <tr key={v.name}>
            <td><img width={64} className={styles.contributorAvatar}
                     src={`https://github.com/${v.githubId}.png`} alt={v.name}/></td>
            <td>{v.inPMC ? <b>{v.name}</b> : v.name}</td>
            <td><a target='_blank' href={`https://people.apache.org/phonebook.html?uid=${v.apacheId}`}>{v.apacheId}</a></td>
            <td><a target='_blank' href={`https://github.com/${v.githubId}`}>{v.githubId}</a></td>
          </tr>
        ))}
      </tbody>
    </table>
  </>
}
